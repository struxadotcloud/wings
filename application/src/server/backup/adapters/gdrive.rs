use crate::{
    io::{
        SafeAsyncWriteExt, SafeDigestExt,
        compression::{CompressionType, reader::CompressionReader},
        limited_reader::{AsyncLimitedReader, LimitedReader},
        limited_writer::LimitedWriter,
    },
    remote::backups::{GDriveBackupConfiguration, RawServerBackup},
    server::{
        backup::{Backup, BackupCleanExt, BackupCreateExt, BackupExt, BackupFindExt},
        filesystem::{
            archive::{Archive, StreamableArchiveFormat},
            virtualfs::{ByteRange, VirtualReadableFilesystem},
        },
    },
    utils::PortablePermissions,
};
use futures::TryStreamExt;
use serde::Deserialize;
use sha2::Digest;
use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

static CLIENT: OnceLock<Arc<reqwest::Client>> = OnceLock::new();

const GDRIVE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const GDRIVE_API: &str = "https://www.googleapis.com/drive/v3/files";
const GDRIVE_UPLOAD: &str = "https://www.googleapis.com/upload/drive/v3/files";
const GDRIVE_CHUNK_SIZE: u64 = 1024 * 1024;

fn get_client(server: &crate::server::Server) -> Arc<reqwest::Client> {
    CLIENT
        .get_or_init(|| {
            Arc::new(
                reqwest::ClientBuilder::new()
                    .timeout(std::time::Duration::from_secs(
                        server
                            .app_state
                            .config
                            .load()
                            .system
                            .backups
                            .gdrive
                            .part_upload_timeout,
                    ))
                    .tls_danger_accept_invalid_certs(
                        server.app_state.config.ignore_certificate_errors,
                    )
                    .build()
                    .expect("failed to build HTTP client"),
            )
        })
        .clone()
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

#[derive(Deserialize)]
struct FileResponse {
    id: String,
}

pub struct GoogleDriveBackup {
    uuid: uuid::Uuid,
}

impl GoogleDriveBackup {
    #[inline]
    fn get_file_name(config: &crate::config::Config, uuid: uuid::Uuid) -> PathBuf {
        config
            .resolve_as_path(|cfg| &cfg.system.backup_directory)
            .join(format!("{uuid}.gdrive.tar.gz"))
    }

    async fn fetch_config(
        server: &crate::server::Server,
        uuid: uuid::Uuid,
    ) -> Result<GDriveBackupConfiguration, anyhow::Error> {
        server
            .app_state
            .config
            .client
            .backup_gdrive_configuration(uuid)
            .await
    }

    async fn refresh_access_token(
        config: &GDriveBackupConfiguration,
    ) -> Result<String, anyhow::Error> {
        static TOKEN_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

        let response: TokenResponse = TOKEN_CLIENT
            .get_or_init(reqwest::Client::new)
            .post(GDRIVE_TOKEN_URL)
            .form(&[
                ("client_id", config.client_id.as_str()),
                ("client_secret", config.client_secret.as_str()),
                ("refresh_token", config.refresh_token.as_str()),
                ("grant_type", "refresh_token"),
            ])
            .send()
            .await?
            .error_for_status()
            .await?
            .json()
            .await?;

        Ok(response.access_token)
    }

    async fn post_session(
        server: &crate::server::Server,
        access_token: &str,
        uuid: uuid::Uuid,
        folder_id: &str,
    ) -> Result<reqwest::Response, anyhow::Error> {
        Ok(get_client(server)
            .post(format!("{}?uploadType=resumable", GDRIVE_UPLOAD))
            .bearer_auth(access_token)
            .header("Content-Type", "application/json; charset=UTF-8")
            .json(&serde_json::json!({
                "name": format!("{uuid}.tar.gz"),
                "parents": [folder_id],
            }))
            .send()
            .await?)
    }

    async fn resumable_init(
        server: &crate::server::Server,
        config: &GDriveBackupConfiguration,
        uuid: uuid::Uuid,
    ) -> Result<String, anyhow::Error> {
        let mut access_token = config.access_token.clone();

        let mut response = Self::post_session(server, &access_token, uuid, &config.folder_id).await?;
        if response.status().as_u16() == 401 {
            access_token = Self::refresh_access_token(config).await?;
            response = Self::post_session(server, &access_token, uuid, &config.folder_id).await?;
        }

        let status = response.status();
        if !status.is_success() {
            return Err(anyhow::anyhow!(
                "failed to start google drive resumable upload: status code {}",
                status
            ));
        }

        response
            .headers()
            .get("Location")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
            .ok_or_else(|| {
                anyhow::anyhow!("google drive resumable upload response missing Location header")
            })
    }

    async fn upload_scratch(
        server: &crate::server::Server,
        config: &GDriveBackupConfiguration,
        file: &mut tokio::fs::File,
        session_url: &str,
        size: u64,
        uuid: uuid::Uuid,
    ) -> Result<String, anyhow::Error> {
        let retry_limit = server.app_state.config.load().system.backups.gdrive.retry_limit;
        let client = get_client(server);

        let mut access_token = config.access_token.clone();
        let mut session_url = session_url.to_string();

        let mut offset = 0u64;
        let mut attempts = 0u64;
        loop {
            attempts += 1;
            if attempts > retry_limit {
                return Err(anyhow::anyhow!(
                    "failed to upload google drive backup after {} attempts",
                    retry_limit
                ));
            }

            let chunk_len = std::cmp::min(GDRIVE_CHUNK_SIZE, size - offset);
            let end = offset + chunk_len - 1;

            tracing::debug!(
                "uploading google drive backup chunk {}-{} of {} for backup {} for {}",
                offset,
                end,
                size,
                uuid,
                server.uuid
            );

            file.seek(std::io::SeekFrom::Start(offset)).await?;
            let reader_handle = file.try_clone().await?;
            let reader = reader_handle.take(chunk_len);
            let reader = AsyncLimitedReader::new_with_bytes_per_second(
                reader,
                server
                    .app_state
                    .config
                    .load()
                    .system
                    .backups
                    .write_limit
                    .as_bytes(),
            );

            let body = reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::with_capacity(
                reader,
                crate::BUFFER_SIZE,
            ));

            match client
                .put(&session_url)
                .bearer_auth(&access_token)
                .header("Content-Length", chunk_len)
                .header("Content-Range", format!("bytes {}-{}/{}", offset, end, size))
                .body(body)
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {
                    let file: FileResponse = response.json().await?;
                    return Ok(file.id);
                }
                Ok(response) if response.status().as_u16() == 308 => {
                    attempts = 0;
                    offset += chunk_len;
                    if offset >= size {
                        break;
                    }
                }
                Ok(response) if response.status().as_u16() == 401 => {
                    access_token = Self::refresh_access_token(config).await?;
                }
                Ok(response) if response.status().as_u16() == 404 => {
                    session_url = Self::resumable_init(server, config, uuid).await?;
                    offset = 0;
                    attempts = 0;
                }
                Ok(response) => {
                    tracing::error!(
                        backup = %uuid,
                        server = %server.uuid,
                        "failed to upload google drive backup chunk {}-{}: status code {}",
                        offset,
                        end,
                        response.status()
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(attempts.pow(2))).await;
                }
                Err(err) => {
                    tracing::error!(
                        backup = %uuid,
                        server = %server.uuid,
                        "failed to upload google drive backup chunk {}-{}: {:#?}",
                        offset,
                        end,
                        err
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(attempts.pow(2))).await;
                }
            }
        }

        let response = client
            .put(&session_url)
            .bearer_auth(&access_token)
            .header("Content-Length", 0)
            .header("Content-Range", format!("bytes */{size}"))
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "google drive resumable upload returned no file id, final status code {}",
                response.status()
            ));
        }

        let file: FileResponse = response.json().await?;
        Ok(file.id)
    }

    async fn create_buffered(
        server: &crate::server::Server,
        uuid: uuid::Uuid,
        progress: crate::server::filesystem::archive::create::ArchiveProgress,
        total: Arc<AtomicU64>,
        ignore: ignore::gitignore::Gitignore,
    ) -> Result<RawServerBackup, anyhow::Error> {
        let file_name = Self::get_file_name(&server.app_state.config, uuid);
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .truncate(true)
            .open(&file_name)
            .await?;

        let (mut checksum_reader, checksum_writer) = tokio::io::simplex(crate::BUFFER_SIZE);

        let checksum_task = async {
            let mut hasher = sha2::Sha256::new();

            let mut buffer = vec![0; crate::BUFFER_SIZE];
            loop {
                let bytes_read = checksum_reader.read(&mut buffer).await?;
                if crate::unlikely(bytes_read == 0) {
                    break;
                }

                hasher.safe_update(&buffer, bytes_read)?;
                file.safe_write_all(&buffer, bytes_read).await?;
                total.fetch_add(bytes_read as u64, Ordering::Relaxed);
            }

            Ok::<_, anyhow::Error>(hex::encode(hasher.finalize()))
        };

        let total_task = {
            let filesystem = server.filesystem.clone();
            let total = Arc::clone(&total);
            let ignore = ignore.clone();

            async move {
                tokio::task::spawn_blocking(move || {
                    let mut walker = filesystem
                        .walk_dir(Path::new(""))?
                        .with_is_ignored(ignore.into());
                    let mut total_files = 0;
                    while let Some(Ok((_, path))) = walker.next_entry() {
                        let metadata = match filesystem.symlink_metadata(&path) {
                            Ok(metadata) => metadata,
                            Err(_) => continue,
                        };

                        total.fetch_add(metadata.len(), Ordering::Relaxed);
                        if !metadata.is_dir() {
                            total_files += 1;
                        }
                    }

                    Ok::<_, anyhow::Error>(total_files)
                })
                .await?
            }
        };

        let archive_task = async {
            let sources = server.filesystem.async_read_dir_all(Path::new("")).await?;
            let writer = tokio_util::io::SyncIoBridge::new(checksum_writer);
            let writer = LimitedWriter::new_with_bytes_per_second(
                writer,
                server
                    .app_state
                    .config
                    .load()
                    .system
                    .backups
                    .write_limit
                    .as_bytes(),
            );

            let file = crate::server::filesystem::archive::create::create_tar(
                server.filesystem.clone(),
                writer,
                Path::new(""),
                sources,
                progress.clone(),
                ignore.into(),
                crate::server::filesystem::archive::create::CreateTarOptions {
                    compression_type: CompressionType::Gz,
                    compression_level: server
                        .app_state
                        .config
                        .load()
                        .system
                        .backups
                        .compression_level,
                    threads: server
                        .app_state
                        .config
                        .load()
                        .system
                        .backups
                        .gdrive
                        .create_threads,
                },
            )
            .await?;

            file.into_inner().into_inner().shutdown().await?;

            Ok(())
        };

        let (checksum, total_files, _) = tokio::try_join!(checksum_task, total_task, archive_task)?;

        let size = file.metadata().await?.len();
        if size == 0 {
            return Err(anyhow::anyhow!(
                "google drive backup archive is 0 bytes, this should not be possible"
            ));
        }

        let config = Self::fetch_config(server, uuid).await?;
        let session_url = Self::resumable_init(server, &config, uuid).await?;
        let file_id = Self::upload_scratch(server, &config, &mut file, &session_url, size, uuid).await?;

        drop(file);
        tokio::fs::remove_file(&file_name).await?;

        Ok(RawServerBackup {
            checksum,
            checksum_type: "sha256".into(),
            size,
            files: total_files,
            successful: true,
            browsable: false,
            streaming: false,
            parts: Vec::new(),
            remote_id: Some(file_id),
        })
    }
}

#[async_trait::async_trait]
impl BackupFindExt for GoogleDriveBackup {
    async fn exists(state: &crate::routes::State, uuid: uuid::Uuid) -> Result<bool, anyhow::Error> {
        let path = Self::get_file_name(&state.config, uuid);
        Ok(tokio::fs::metadata(&path).await.is_ok())
    }

    async fn find(
        _state: &crate::routes::State,
        uuid: uuid::Uuid,
    ) -> Result<Option<Backup>, anyhow::Error> {
        Ok(Some(Backup::GoogleDrive(GoogleDriveBackup { uuid })))
    }
}

#[async_trait::async_trait]
impl BackupCreateExt for GoogleDriveBackup {
    async fn create(
        server: &crate::server::Server,
        uuid: uuid::Uuid,
        progress: crate::server::filesystem::archive::create::ArchiveProgress,
        total: Arc<AtomicU64>,
        ignore: ignore::gitignore::Gitignore,
        _ignore_raw: compact_str::CompactString,
    ) -> Result<RawServerBackup, anyhow::Error> {
        Self::create_buffered(server, uuid, progress, total, ignore).await
    }
}

#[async_trait::async_trait]
impl BackupExt for GoogleDriveBackup {
    #[inline]
    fn uuid(&self) -> uuid::Uuid {
        self.uuid
    }

    async fn download_info(
        &self,
    ) -> Result<crate::server::backup::BackupDownloadInfo, anyhow::Error> {
        Err(anyhow::anyhow!(
            "this backup adapter does not support downloads"
        ))
    }

    async fn download(
        &self,
        _state: &crate::routes::State,
        _archive_format: StreamableArchiveFormat,
        _range: Option<ByteRange>,
    ) -> Result<crate::response::ApiResponse, anyhow::Error> {
        Err(anyhow::anyhow!(
            "this backup adapter does not support downloads"
        ))
    }

    async fn restore(
        &self,
        server: &crate::server::Server,
        progress: crate::server::filesystem::archive::create::ArchiveProgress,
        total: Arc<AtomicU64>,
        _download_url: Option<compact_str::CompactString>,
    ) -> Result<(), anyhow::Error> {
        let config = Self::fetch_config(server, self.uuid).await?;
        let file_id = config
            .file_id
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("google drive backup file id not found"))?;

        let response = get_client(server)
            .get(format!("{}/{}?alt=media", GDRIVE_API, file_id))
            .bearer_auth(&config.access_token)
            .send()
            .await?;

        let response = if response.status().as_u16() == 401 {
            let access_token = Self::refresh_access_token(&config).await?;
            get_client(server)
                .get(format!("{}/{}?alt=media", GDRIVE_API, file_id))
                .bearer_auth(&access_token)
                .send()
                .await?
        } else {
            response
        };

        let status = response.status();
        if status.as_u16() == 404 {
            return Err(anyhow::anyhow!(
                "backup file no longer exists in Google Drive"
            ));
        }
        if !status.is_success() {
            return Err(anyhow::anyhow!(
                "failed to download google drive backup: status code {}",
                status
            ));
        }

        if let Some(content_length) = response.content_length() {
            total.store(content_length, Ordering::SeqCst);
        }

        let reader = tokio_util::io::StreamReader::new(Box::pin(
            response.bytes_stream().map_err(std::io::Error::other),
        ));

        let server = server.clone();

        tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
            let reader = tokio_util::io::SyncIoBridge::new(reader);
            let reader = LimitedReader::new_with_bytes_per_second(
                reader,
                server.app_state.config.load().system.backups.read_limit.as_bytes(),
            );
            let reader = progress.counting_reader(reader);
            let reader = CompressionReader::new(reader, CompressionType::Gz)?;
            let reader = std::io::BufReader::with_capacity(crate::TRANSFER_BUFFER_SIZE, reader);

            let mut archive = tar::Archive::new(reader);
            let mut directory_entries = chunked_vec::ChunkedVec::new();
            let mut last_parent = None;
            let entries = archive.entries()?;

            let mut read_buffer = vec![0; crate::TRANSFER_BUFFER_SIZE];
            for entry in entries {
                let mut entry = entry?;
                let path = server.filesystem.relative_path(&entry.path()?);

                if path.as_os_str().is_empty() {
                    continue;
                }

                let header = entry.header();
                match header.entry_type() {
                    tar::EntryType::Directory => {
                        server.filesystem.create_chowned_dir_all(path.as_path())?;
                        server
                            .filesystem
                            .set_permissions(
                                path.as_path(),
                                PortablePermissions::from_mode_dir(header.mode().unwrap_or(0o755)),
                            )?;

                        if let Ok(modified_time) = header.mtime() && directory_entries.len() < Archive::MAX_DIRECTORY_MTIME_ENTRIES {
                            directory_entries.push((path.to_path_buf(), modified_time));
                        }
                    }
                    tar::EntryType::Regular => {
                        server.log_daemon(compact_str::format_compact!("(restoring): {}", path.display()));

                        if let Some(parent) = path.parent()
                            && last_parent.as_deref() != Some(parent)
                        {
                            server.filesystem.create_chowned_dir_all(parent)?;
                            last_parent = Some(parent.to_path_buf());
                        }

                        let mut writer = crate::server::filesystem::file::ServerFile::new(
                            server.clone(),
                            &path,
                            Some(PortablePermissions::from_mode_file(header.mode().unwrap_or(0o644))),
                            header
                                .mtime()
                                .map(|t| std::time::UNIX_EPOCH + std::time::Duration::from_secs(t))
                                .ok(),
                        )?;

                        crate::io::copy_shared(&mut read_buffer, &mut entry, &mut writer)?;
                        writer.flush()?;

                        progress.increment_files();
                    }
                    tar::EntryType::Symlink => {
                        let link = entry.link_name().unwrap_or_default().unwrap_or_default();

                        if let Err(err) = server.filesystem.symlink(link, path.as_path()) {
                            tracing::debug!(path = %path.display(), "failed to create symlink from backup: {:?}", err);
                        } else {
                            progress.increment_files();

                            if let Ok(modified_time) = header.mtime() {
                                server
                                    .filesystem
                                    .set_times(
                                        path.as_path(),
                                        std::time::UNIX_EPOCH
                                            + std::time::Duration::from_secs(modified_time),
                                        None,
                                    )?;
                            }
                        }
                    }
                    _ => {}
                }
            }

            for (destination_path, modified_time) in directory_entries {
                server.filesystem.set_times(
                    &destination_path,
                    std::time::UNIX_EPOCH + std::time::Duration::from_secs(modified_time),
                    None,
                )?;
            }

            Ok(())
        })
        .await??;

        Ok(())
    }

    async fn delete(&self, state: &crate::routes::State) -> Result<(), anyhow::Error> {
        let Ok(config) = state
            .config
            .client
            .backup_gdrive_configuration(self.uuid)
            .await
        else {
            return Ok(());
        };
        let Some(file_id) = config.file_id else {
            return Ok(());
        };

        let Ok(client) = reqwest::ClientBuilder::new()
            .timeout(std::time::Duration::from_secs(
                state.config.load().system.backups.gdrive.part_upload_timeout,
            ))
            .tls_danger_accept_invalid_certs(state.config.ignore_certificate_errors)
            .build()
        else {
            return Ok(());
        };

        let response = client
            .delete(format!("{}/{}", GDRIVE_API, file_id))
            .bearer_auth(&config.access_token)
            .send()
            .await;
        let needs_refresh = matches!(&response, Ok(response) if response.status().as_u16() == 401);
        if needs_refresh
            && let Ok(access_token) = Self::refresh_access_token(&config).await
        {
            let _ = client
                .delete(format!("{}/{}", GDRIVE_API, file_id))
                .bearer_auth(&access_token)
                .send()
                .await;
        }

        Ok(())
    }

    async fn browse(
        &self,
        _server: &crate::server::Server,
    ) -> Result<Arc<dyn VirtualReadableFilesystem>, anyhow::Error> {
        Err(anyhow::anyhow!(
            "this backup adapter does not support browsing files"
        ))
    }
}

#[async_trait::async_trait]
impl BackupCleanExt for GoogleDriveBackup {
    async fn clean(server: &crate::server::Server, uuid: uuid::Uuid) -> Result<(), anyhow::Error> {
        let file_name = Self::get_file_name(&server.app_state.config, uuid);
        if tokio::fs::metadata(&file_name).await.is_ok() {
            tokio::fs::remove_file(&file_name).await?;
        }

        Ok(())
    }
}
