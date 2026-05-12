use crate::{
    io::{
        compression::{CompressionLevel, writer::CompressionWriter},
        counting_reader::CountingReader,
        fixed_reader::FixedReader,
    },
    models::DirectoryEntry,
    routes::MimeCacheValue,
    server::filesystem::{
        archive::StreamableArchiveFormat,
        cap::FileType,
        encode_mode,
        usage::SpaceDelta,
        virtualfs::{
            AsyncFileRead, AsyncReadableFileStream, ByteRange, DirectoryListing,
            DirectoryStreamWalk, DirectoryWalk, FileMetadata, FileRead, IsIgnoredFn,
            ReadableFileStream, VirtualReadableFilesystem,
        },
    },
    utils::PortablePermissions,
};
use chrono::{Datelike, Timelike};
use ddup_bak::archive::entries::Entry;
use itaf::encoder::{EncoderOptions, ItafEncoder, Metadata};
use std::{
    collections::VecDeque,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicU64},
};
use tokio::io::AsyncWriteExt;

trait EntryReaderExt {
    fn entry_reader(
        &self,
        entry: ddup_bak::archive::entries::Entry,
    ) -> Result<ReadableFileStream, anyhow::Error>;
}

impl EntryReaderExt for Option<Arc<ddup_bak::repository::Repository>> {
    fn entry_reader(
        &self,
        entry: ddup_bak::archive::entries::Entry,
    ) -> Result<ReadableFileStream, anyhow::Error> {
        Ok(if let Some(repository) = self {
            Box::new(repository.entry_reader(entry)?)
        } else {
            match entry {
                ddup_bak::archive::entries::Entry::File(file) => Box::new(file),
                _ => {
                    return Err(anyhow::anyhow!("Entry reader is only available for files"));
                }
            }
        })
    }
}

fn entry_size_recursive(entry: &Entry) -> (u64, u64) {
    match entry {
        Entry::File(file) => (
            file.size_real,
            file.size_compressed.unwrap_or(file.size_real),
        ),
        Entry::Directory(dir) => dir
            .entries
            .iter()
            .map(entry_size_recursive)
            .fold((0, 0), |acc, x| (acc.0 + x.0, acc.1 + x.1)),
        Entry::Symlink(link) => (link.target.len() as u64, link.target.len() as u64),
    }
}

pub trait CmpSortExt {
    fn cmp_sort(
        &self,
        other: &Self,
        sort: crate::models::DirectorySortingMode,
    ) -> std::cmp::Ordering;
}

impl CmpSortExt for Entry {
    fn cmp_sort(
        &self,
        other: &Self,
        sort: crate::models::DirectorySortingMode,
    ) -> std::cmp::Ordering {
        use crate::models::DirectorySortingMode::*;

        match sort {
            NameAsc => self.name().cmp(other.name()),
            NameDesc => other.name().cmp(self.name()),
            SizeAsc | SizeDesc | PhysicalSizeAsc | PhysicalSizeDesc => {
                let (a_log, a_phy) = entry_size_recursive(self);
                let (b_log, b_phy) = entry_size_recursive(other);
                match sort {
                    SizeAsc => a_log.cmp(&b_log),
                    SizeDesc => b_log.cmp(&a_log),
                    PhysicalSizeAsc => a_phy.cmp(&b_phy),
                    PhysicalSizeDesc => b_phy.cmp(&a_phy),
                    _ => unreachable!(),
                }
            }
            ModifiedAsc | CreatedAsc => self.mtime().cmp(&other.mtime()),
            ModifiedDesc | CreatedDesc => other.mtime().cmp(&self.mtime()),
        }
    }
}

#[derive(Clone)]
pub struct VirtualDdupBakArchive {
    pub server: crate::server::Server,
    pub archive: Arc<ddup_bak::archive::Archive>,
    pub archive_created: chrono::DateTime<chrono::Utc>,
    pub repository: Option<Arc<ddup_bak::repository::Repository>>,
    pub sizes: Arc<crate::server::filesystem::usage::DiskUsage>,
}

fn sort_dir_entries_by_size(
    entries: &mut Vec<&Entry>,
    sort: crate::models::DirectorySortingMode,
    sizes: &crate::server::filesystem::usage::DiskUsage,
    parent_path: &Path,
) {
    use crate::models::DirectorySortingMode::*;
    match sort {
        SizeAsc | SizeDesc | PhysicalSizeAsc | PhysicalSizeDesc => {
            entries.sort_unstable_by(|a, b| {
                let a_space = sizes
                    .get_size(&parent_path.join(a.name()))
                    .unwrap_or_default();
                let b_space = sizes
                    .get_size(&parent_path.join(b.name()))
                    .unwrap_or_default();
                match sort {
                    SizeAsc => a_space.get_logical().cmp(&b_space.get_logical()),
                    SizeDesc => b_space.get_logical().cmp(&a_space.get_logical()),
                    PhysicalSizeAsc => a_space.get_physical().cmp(&b_space.get_physical()),
                    PhysicalSizeDesc => b_space.get_physical().cmp(&a_space.get_physical()),
                    _ => unreachable!(),
                }
            });
        }
        _ => entries.sort_unstable_by(|a, b| a.cmp_sort(b, sort)),
    }
}

fn populate_disk_usage(
    entries: &[Entry],
    prefix: &Path,
    sizes: &mut crate::server::filesystem::usage::DiskUsage,
) {
    for entry in entries {
        let entry_path = prefix.join(entry.name());
        match entry {
            Entry::File(file) => {
                let delta = SpaceDelta::new(
                    file.size_real as i64,
                    file.size_compressed.unwrap_or(file.size_real) as i64,
                );
                sizes.update_size(prefix, delta);
            }
            Entry::Directory(dir) => {
                sizes.update_size(&entry_path, SpaceDelta::new(0, 0));
                populate_disk_usage(&dir.entries, &entry_path, sizes);
            }
            Entry::Symlink(link) => {
                let len = link.target.len() as i64;
                sizes.update_size(prefix, SpaceDelta::new(len, len));
            }
        }
    }
}

impl VirtualDdupBakArchive {
    pub fn new(
        server: crate::server::Server,
        archive: Arc<ddup_bak::archive::Archive>,
        archive_created: chrono::DateTime<chrono::Utc>,
        repository: Option<Arc<ddup_bak::repository::Repository>>,
    ) -> Self {
        let mut sizes = crate::server::filesystem::usage::DiskUsage::default();
        populate_disk_usage(archive.entries(), Path::new(""), &mut sizes);

        Self {
            server,
            archive,
            archive_created,
            repository,
            sizes: Arc::new(sizes),
        }
    }

    pub async fn open(
        server: crate::server::Server,
        archive_path: &Path,
    ) -> Result<Self, anyhow::Error> {
        let file = server
            .filesystem
            .async_open(archive_path)
            .await?
            .into_std()
            .await;
        let archive =
            tokio::task::spawn_blocking(move || ddup_bak::archive::Archive::open_file(file))
                .await??;

        let metadata = server.filesystem.async_metadata(archive_path).await?;

        Ok(Self::new(
            server,
            Arc::new(archive),
            metadata
                .created()
                .map_or_else(|_| Default::default(), |dt| dt.into_std().into()),
            None,
        ))
    }

    fn ddup_bak_entry_to_directory_entry(
        archive_created: &chrono::DateTime<chrono::Utc>,
        path: &Path,
        entry: &ddup_bak::archive::entries::Entry,
    ) -> DirectoryEntry {
        let (size, size_physical) = entry_size_recursive(entry);

        let detected_mime = if entry.is_directory() {
            MimeCacheValue::directory()
        } else if entry.is_symlink() {
            MimeCacheValue::symlink()
        } else {
            crate::utils::detect_mime_type(path, None)
        };

        DirectoryEntry {
            name: path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into(),
            mode: encode_mode(entry.mode().bits()),
            mode_bits: compact_str::format_compact!("{:o}", entry.mode().bits() & 0o777),
            size,
            size_physical,
            editable: entry.is_file() && detected_mime.valid_utf8,
            inner_editable: entry.is_file() && detected_mime.valid_inner_utf8,
            directory: entry.is_directory(),
            file: entry.is_file(),
            symlink: entry.is_symlink(),
            mime: detected_mime.mime,
            modified: chrono::DateTime::from_timestamp(
                entry
                    .mtime()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64,
                0,
            )
            .unwrap_or_default(),
            created: *archive_created,
        }
    }

    fn ddup_bak_entry_to_file_type(entry: &ddup_bak::archive::entries::Entry) -> FileType {
        match entry {
            ddup_bak::archive::entries::Entry::Directory(_) => FileType::Dir,
            ddup_bak::archive::entries::Entry::File(_) => FileType::File,
            ddup_bak::archive::entries::Entry::Symlink(_) => FileType::Symlink,
        }
    }

    fn tar_recursive_convert_entries(
        entry: &Entry,
        repository: &Option<Arc<ddup_bak::repository::Repository>>,
        archive: &mut tar::Builder<impl Write + 'static>,
        parent_path: &Path,
        bytes_archived: &Option<Arc<AtomicU64>>,
        is_ignored: &IsIgnoredFn,
    ) -> Result<(), anyhow::Error> {
        let path = parent_path.join(entry.name());

        let Some(path) = (is_ignored)(Self::ddup_bak_entry_to_file_type(entry), path) else {
            return Ok(());
        };

        let mut entry_header = tar::Header::new_gnu();
        entry_header.set_size(0);
        entry_header.set_mode(entry.mode().bits());
        entry_header.set_mtime(
            entry
                .mtime()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
        );

        match entry {
            Entry::Directory(dir) => {
                entry_header.set_entry_type(tar::EntryType::Directory);

                archive.append_data(&mut entry_header, &path, std::io::empty())?;

                for entry in dir.entries.iter() {
                    Self::tar_recursive_convert_entries(
                        entry,
                        repository,
                        archive,
                        &path,
                        bytes_archived,
                        is_ignored,
                    )?;
                }
            }
            Entry::File(file) => {
                entry_header.set_entry_type(tar::EntryType::Regular);
                entry_header.set_size(file.size_real);

                let reader: Box<dyn Read> = match &bytes_archived {
                    Some(bytes_archived) => Box::new(CountingReader::new_with_bytes_read(
                        repository.entry_reader(Entry::File(file.clone()))?,
                        Arc::clone(bytes_archived),
                    )),
                    None => Box::new(repository.entry_reader(Entry::File(file.clone()))?),
                };
                let reader = FixedReader::new_with_fixed_bytes(reader, file.size_real as usize);

                archive.append_data(&mut entry_header, &path, reader)?;
            }
            Entry::Symlink(link) => {
                entry_header.set_entry_type(tar::EntryType::Symlink);

                archive.append_link(&mut entry_header, &path, &link.target)?;
            }
        }

        Ok(())
    }

    fn itaf_recursive_convert_entries<W: Write>(
        entry: &Entry,
        repository: &Option<Arc<ddup_bak::repository::Repository>>,
        itaf_enc: &mut ItafEncoder<W>,
        parent_path: &Path,
        bytes_archived: &Option<Arc<AtomicU64>>,
        is_ignored: &IsIgnoredFn,
    ) -> Result<(), anyhow::Error> {
        let path = parent_path.join(entry.name());

        let Some(path) = (is_ignored)(Self::ddup_bak_entry_to_file_type(entry), path) else {
            return Ok(());
        };

        let mtime = entry
            .mtime()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let meta = Metadata {
            uid: 0,
            gid: 0,
            mode: entry.mode().bits(),
            modified: std::time::UNIX_EPOCH + mtime,
        };
        let name: compact_str::CompactString = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into();

        match entry {
            Entry::Directory(dir) => {
                if itaf::spec::validate_name(&name).is_ok() {
                    itaf_enc.enter_dir(&name, &meta)?;
                    for child in &dir.entries {
                        Self::itaf_recursive_convert_entries(
                            child,
                            repository,
                            itaf_enc,
                            &path,
                            bytes_archived,
                            is_ignored,
                        )?;
                    }
                    itaf_enc.exit_dir()?;
                }
            }
            Entry::File(file) => {
                if itaf::spec::validate_name(&name).is_ok() {
                    let reader: Box<dyn Read> = match bytes_archived {
                        Some(ba) => Box::new(CountingReader::new_with_bytes_read(
                            repository.entry_reader(Entry::File(file.clone()))?,
                            Arc::clone(ba),
                        )),
                        None => Box::new(repository.entry_reader(Entry::File(file.clone()))?),
                    };
                    let mut reader =
                        FixedReader::new_with_fixed_bytes(reader, file.size_real as usize);
                    itaf_enc.add_file(&name, &meta, file.size_real, &mut { reader })?;
                }
            }
            Entry::Symlink(link) => {
                if itaf::spec::validate_name(&name).is_ok() {
                    itaf_enc.add_symlink(&name, &link.target, false, &meta)?;
                }
            }
        }

        Ok(())
    }

    fn zip_recursive_convert_entries(
        entry: &Entry,
        repository: &Option<Arc<ddup_bak::repository::Repository>>,
        zip: &mut zip::ZipWriter<
            zip::write::StreamWriter<
                tokio_util::io::SyncIoBridge<tokio::io::WriteHalf<tokio::io::SimplexStream>>,
            >,
        >,
        compression_level: CompressionLevel,
        parent_path: &Path,
        bytes_archived: &Option<Arc<AtomicU64>>,
        is_ignored: &IsIgnoredFn,
    ) -> Result<(), anyhow::Error> {
        let path = parent_path.join(entry.name());

        let Some(path) = (is_ignored)(Self::ddup_bak_entry_to_file_type(entry), path) else {
            return Ok(());
        };

        let size = match entry {
            Entry::File(file) => file.size,
            _ => 0,
        };

        let mut options: zip::write::FileOptions<'_, ()> = zip::write::FileOptions::default()
            .compression_level(Some(compression_level.to_deflate_level() as i64))
            .unix_permissions(entry.mode().bits())
            .large_file(size >= u32::MAX as u64);
        {
            let mtime: chrono::DateTime<chrono::Utc> = chrono::DateTime::from(entry.mtime());

            options = options.last_modified_time(zip::DateTime::from_date_and_time(
                mtime.year() as u16,
                mtime.month() as u8,
                mtime.day() as u8,
                mtime.hour() as u8,
                mtime.minute() as u8,
                mtime.second() as u8,
            )?);
        }

        match entry {
            Entry::Directory(dir) => {
                zip.add_directory(path.to_string_lossy(), options)?;

                for entry in dir.entries.iter() {
                    Self::zip_recursive_convert_entries(
                        entry,
                        repository,
                        zip,
                        compression_level,
                        &path,
                        bytes_archived,
                        is_ignored,
                    )?;
                }
            }
            Entry::File(file) => {
                let reader: Box<dyn Read> = match &bytes_archived {
                    Some(bytes_archived) => Box::new(CountingReader::new_with_bytes_read(
                        repository.entry_reader(Entry::File(file.clone()))?,
                        Arc::clone(bytes_archived),
                    )),
                    None => Box::new(repository.entry_reader(Entry::File(file.clone()))?),
                };
                let mut reader = FixedReader::new_with_fixed_bytes(reader, file.size_real as usize);

                zip.start_file(path.to_string_lossy(), options)?;
                crate::io::copy(&mut reader, zip)?;
            }
            Entry::Symlink(link) => {
                zip.add_symlink(&link.name, &link.target, options)?;
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl VirtualReadableFilesystem for VirtualDdupBakArchive {
    fn backing_server(&self) -> &crate::server::Server {
        &self.server
    }

    fn metadata(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
    ) -> Result<FileMetadata, anyhow::Error> {
        if path.as_ref() == Path::new("") || path.as_ref() == Path::new("/") {
            return Ok(FileMetadata {
                file_type: FileType::Dir,
                permissions: PortablePermissions::from_mode(0o755),
                size: 0,
                modified: None,
                created: None,
            });
        }

        let archive = self.archive.clone();
        let path = path.as_ref().to_path_buf();

        let entry = archive.find_archive_entry(&path).ok_or_else(|| {
            anyhow::anyhow!(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "File not found"
            ))
        })?;

        Ok(FileMetadata {
            file_type: Self::ddup_bak_entry_to_file_type(entry),
            permissions: PortablePermissions::from_mode(entry.mode().bits() & 0o777),
            size: match &entry {
                ddup_bak::archive::entries::Entry::File(f) => f.size_real,
                _ => 0,
            },
            modified: Some(
                std::time::SystemTime::UNIX_EPOCH
                    + entry.mtime().duration_since(std::time::UNIX_EPOCH)?,
            ),
            created: None,
        })
    }
    async fn async_metadata(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
    ) -> Result<FileMetadata, anyhow::Error> {
        self.metadata(path)
    }

    fn symlink_metadata(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
    ) -> Result<FileMetadata, anyhow::Error> {
        self.metadata(path)
    }
    async fn async_symlink_metadata(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
    ) -> Result<FileMetadata, anyhow::Error> {
        self.metadata(path)
    }

    async fn async_directory_entry(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
    ) -> Result<DirectoryEntry, anyhow::Error> {
        let archive = self.archive.clone();
        let path = path.as_ref().to_path_buf();

        let entry = archive.find_archive_entry(&path).ok_or_else(|| {
            anyhow::anyhow!(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "File not found"
            ))
        })?;

        Ok(Self::ddup_bak_entry_to_directory_entry(
            &self.archive_created,
            &path,
            entry,
        ))
    }

    async fn async_directory_entry_buffer(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
        _buffer: &[u8],
    ) -> Result<DirectoryEntry, anyhow::Error> {
        self.async_directory_entry(path).await
    }

    async fn async_read_dir(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
        per_page: Option<usize>,
        page: usize,
        is_ignored: IsIgnoredFn,
        sort: crate::models::DirectorySortingMode,
    ) -> Result<DirectoryListing, anyhow::Error> {
        let archive = self.archive.clone();
        let archive_created = self.archive_created;
        let sizes = self.sizes.clone();
        let path = path.as_ref().to_path_buf();

        let entries =
            tokio::task::spawn_blocking(move || -> Result<DirectoryListing, anyhow::Error> {
                let entry = match archive.find_archive_entry(&path) {
                    Some(entry) => entry,
                    None => {
                        let directory_entry_count = archive
                            .entries()
                            .iter()
                            .filter(|e| e.is_directory())
                            .count();

                        let mut directory_entries = Vec::new();
                        directory_entries.reserve_exact(directory_entry_count);
                        let mut other_entries = Vec::new();
                        other_entries
                            .reserve_exact(archive.entries().len() - directory_entry_count);

                        for entry in archive.entries() {
                            if (is_ignored)(
                                Self::ddup_bak_entry_to_file_type(entry),
                                PathBuf::from(entry.name()),
                            )
                            .is_none()
                            {
                                continue;
                            }

                            if entry.is_directory() {
                                directory_entries.push(entry);
                            } else {
                                other_entries.push(entry);
                            }
                        }

                        sort_dir_entries_by_size(&mut directory_entries, sort, &sizes, &path);
                        other_entries.sort_unstable_by(|a, b| a.cmp_sort(b, sort));

                        let total_entries = directory_entries.len() + other_entries.len();
                        let mut entries = Vec::new();

                        if let Some(per_page) = per_page {
                            let start = (page - 1) * per_page;

                            for entry in directory_entries
                                .into_iter()
                                .chain(other_entries)
                                .skip(start)
                                .take(per_page)
                            {
                                let path = path.join(entry.name());
                                entries.push(Self::ddup_bak_entry_to_directory_entry(
                                    &archive_created,
                                    &path,
                                    entry,
                                ));
                            }
                        } else {
                            for entry in directory_entries.into_iter().chain(other_entries) {
                                let path = path.join(entry.name());
                                entries.push(Self::ddup_bak_entry_to_directory_entry(
                                    &archive_created,
                                    &path,
                                    entry,
                                ));
                            }
                        }

                        return Ok(DirectoryListing {
                            total_entries,
                            entries,
                        });
                    }
                };

                match entry {
                    ddup_bak::archive::entries::Entry::Directory(dir) => {
                        let mut directory_entries = Vec::new();
                        directory_entries
                            .reserve_exact(dir.entries.iter().filter(|e| e.is_directory()).count());
                        let mut other_entries = Vec::new();
                        other_entries.reserve_exact(
                            dir.entries.iter().filter(|e| !e.is_directory()).count(),
                        );

                        for entry in &dir.entries {
                            if (is_ignored)(
                                Self::ddup_bak_entry_to_file_type(entry),
                                PathBuf::from(entry.name()),
                            )
                            .is_none()
                            {
                                continue;
                            }

                            if entry.is_directory() {
                                directory_entries.push(entry);
                            } else {
                                other_entries.push(entry);
                            }
                        }

                        sort_dir_entries_by_size(&mut directory_entries, sort, &sizes, &path);
                        other_entries.sort_unstable_by(|a, b| a.cmp_sort(b, sort));

                        let total_entries = directory_entries.len() + other_entries.len();
                        let mut entries = Vec::new();

                        if let Some(per_page) = per_page {
                            let start = (page - 1) * per_page;

                            for entry in directory_entries
                                .into_iter()
                                .chain(other_entries)
                                .skip(start)
                                .take(per_page)
                            {
                                let path = path.join(entry.name());
                                entries.push(Self::ddup_bak_entry_to_directory_entry(
                                    &archive_created,
                                    &path,
                                    entry,
                                ));
                            }
                        } else {
                            for entry in directory_entries.into_iter().chain(other_entries) {
                                let path = path.join(entry.name());
                                entries.push(Self::ddup_bak_entry_to_directory_entry(
                                    &archive_created,
                                    &path,
                                    entry,
                                ));
                            }
                        }

                        Ok(DirectoryListing {
                            total_entries,
                            entries,
                        })
                    }
                    _ => Err(anyhow::anyhow!("Expected a directory entry")),
                }
            })
            .await??;

        Ok(entries)
    }

    fn read_file(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
        _range: Option<ByteRange>,
    ) -> Result<FileRead, anyhow::Error> {
        let archive = self.archive.clone();
        let path = path.as_ref().to_path_buf();

        let entry = archive.find_archive_entry(&path).ok_or_else(|| {
            anyhow::anyhow!(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "File not found"
            ))
        })?;

        let size = match entry {
            ddup_bak::archive::entries::Entry::File(file) => file.size_real,
            _ => return Err(anyhow::anyhow!("Not a file")),
        };

        let entry_reader = self.repository.entry_reader(entry.clone())?;

        Ok(FileRead {
            size,
            total_size: size,
            reader_range: None,
            reader: entry_reader,
        })
    }
    async fn async_read_file(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
        _range: Option<ByteRange>,
    ) -> Result<AsyncFileRead, anyhow::Error> {
        let archive = self.archive.clone();
        let path = path.as_ref().to_path_buf();

        let entry = archive.find_archive_entry(&path).ok_or_else(|| {
            anyhow::anyhow!(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "File not found"
            ))
        })?;

        let size = match entry {
            ddup_bak::archive::entries::Entry::File(file) => file.size_real,
            _ => return Err(anyhow::anyhow!("Not a file")),
        };

        let mut entry_reader = self.repository.entry_reader(entry.clone())?;
        let (reader, mut writer) = tokio::io::simplex(crate::BUFFER_SIZE);

        tokio::task::spawn_blocking(move || {
            let runtime = tokio::runtime::Handle::current();
            let mut buffer = vec![0; crate::BUFFER_SIZE];
            loop {
                match entry_reader.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(n) => {
                        if runtime.block_on(writer.write_all(&buffer[..n])).is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        tracing::error!("error reading from ddup_bak entry: {:?}", err);
                        break;
                    }
                }
            }

            runtime.block_on(writer.shutdown()).ok();
        });

        Ok(AsyncFileRead {
            size,
            total_size: size,
            reader_range: None,
            reader: Box::new(reader),
        })
    }

    fn read_symlink(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
    ) -> Result<PathBuf, anyhow::Error> {
        let archive = self.archive.clone();
        let entry = archive
            .find_archive_entry(path.as_ref())
            .ok_or_else(|| anyhow::anyhow!("Entry not found"))?;

        match entry {
            ddup_bak::archive::entries::Entry::Symlink(link) => Ok(PathBuf::from(&link.target)),
            _ => Err(anyhow::anyhow!("Not a symlink")),
        }
    }
    async fn async_read_symlink(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
    ) -> Result<PathBuf, anyhow::Error> {
        self.read_symlink(path)
    }

    async fn async_read_dir_archive(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
        archive_format: StreamableArchiveFormat,
        compression_level: CompressionLevel,
        bytes_archived: Option<Arc<AtomicU64>>,
        is_ignored: IsIgnoredFn,
    ) -> Result<tokio::io::ReadHalf<tokio::io::SimplexStream>, anyhow::Error> {
        let archive = self.archive.clone();
        let path = path.as_ref().to_path_buf();
        let repository = self.repository.clone();

        let (simplex_reader, writer) = tokio::io::simplex(crate::BUFFER_SIZE);

        match archive_format {
            StreamableArchiveFormat::Zip => {
                crate::spawn_blocking_handled(move || -> Result<(), anyhow::Error> {
                    let writer = tokio_util::io::SyncIoBridge::new(writer);
                    let mut zip = zip::ZipWriter::new_stream(writer);

                    match archive.find_archive_entry(&path) {
                        Some(entry) => {
                            let entry = match entry {
                                ddup_bak::archive::entries::Entry::Directory(entry) => entry,
                                _ => {
                                    return Err(anyhow::anyhow!(std::io::Error::new(
                                        std::io::ErrorKind::NotFound,
                                        "File not found"
                                    )));
                                }
                            };

                            for entry in entry.entries.iter() {
                                Self::zip_recursive_convert_entries(
                                    entry,
                                    &repository,
                                    &mut zip,
                                    compression_level,
                                    Path::new(""),
                                    &bytes_archived,
                                    &is_ignored,
                                )?;
                            }
                        }
                        None => {
                            if path.components().count() == 0 {
                                for entry in archive.entries() {
                                    Self::zip_recursive_convert_entries(
                                        entry,
                                        &repository,
                                        &mut zip,
                                        compression_level,
                                        Path::new(""),
                                        &bytes_archived,
                                        &is_ignored,
                                    )?;
                                }
                            }
                        }
                    };

                    let mut inner = zip.finish()?.into_inner();
                    inner.flush()?;
                    inner.shutdown()?;

                    Ok(())
                });
            }
            f if f.is_tar() => {
                let writer = CompressionWriter::new(
                    tokio_util::io::SyncIoBridge::new(writer),
                    f.compression_format(),
                    compression_level,
                    self.server.app_state.config.api.file_compression_threads,
                )?;

                crate::spawn_blocking_handled(move || -> Result<(), anyhow::Error> {
                    let mut tar = tar::Builder::new(writer);
                    tar.mode(tar::HeaderMode::Complete);

                    match archive.find_archive_entry(&path) {
                        Some(entry) => {
                            let entry = match entry {
                                ddup_bak::archive::entries::Entry::Directory(entry) => entry,
                                _ => {
                                    return Err(anyhow::anyhow!(std::io::Error::new(
                                        std::io::ErrorKind::NotFound,
                                        "File not found"
                                    )));
                                }
                            };

                            for entry in entry.entries.iter() {
                                Self::tar_recursive_convert_entries(
                                    entry,
                                    &repository,
                                    &mut tar,
                                    Path::new(""),
                                    &bytes_archived,
                                    &is_ignored,
                                )?;
                            }
                        }
                        None => {
                            if path.components().count() == 0 {
                                for entry in archive.entries() {
                                    Self::tar_recursive_convert_entries(
                                        entry,
                                        &repository,
                                        &mut tar,
                                        Path::new(""),
                                        &bytes_archived,
                                        &is_ignored,
                                    )?;
                                }
                            }
                        }
                    };

                    tar.finish()?;
                    let mut inner = tar.into_inner()?.finish()?;
                    inner.flush()?;
                    inner.shutdown()?;

                    Ok(())
                });
            }
            f if f.is_itaf() => {
                let writer = CompressionWriter::new(
                    tokio_util::io::SyncIoBridge::new(writer),
                    f.compression_format(),
                    compression_level,
                    self.server.app_state.config.api.file_compression_threads,
                )?;

                crate::spawn_blocking_handled(move || -> Result<(), anyhow::Error> {
                    let mut itaf_enc = ItafEncoder::new(
                        writer,
                        EncoderOptions {
                            base_timestamp: None,
                            crc_enabled: true,
                        },
                    )?;

                    match archive.find_archive_entry(&path) {
                        Some(entry) => {
                            let entry = match entry {
                                ddup_bak::archive::entries::Entry::Directory(entry) => entry,
                                _ => {
                                    return Err(anyhow::anyhow!(std::io::Error::new(
                                        std::io::ErrorKind::NotFound,
                                        "File not found"
                                    )));
                                }
                            };

                            for entry in entry.entries.iter() {
                                Self::itaf_recursive_convert_entries(
                                    entry,
                                    &repository,
                                    &mut itaf_enc,
                                    Path::new(""),
                                    &bytes_archived,
                                    &is_ignored,
                                )?;
                            }
                        }
                        None => {
                            if path.components().count() == 0 {
                                for entry in archive.entries() {
                                    Self::itaf_recursive_convert_entries(
                                        entry,
                                        &repository,
                                        &mut itaf_enc,
                                        Path::new(""),
                                        &bytes_archived,
                                        &is_ignored,
                                    )?;
                                }
                            }
                        }
                    };

                    let mut inner = itaf_enc.finish()?.finish()?;
                    inner.flush()?;
                    inner.shutdown()?;

                    Ok(())
                });
            }
            _ => {
                tracing::error!(
                    "unsupported archive format for ddup_bak vfs: {}",
                    archive_format.extension()
                );
            }
        }

        Ok(simplex_reader)
    }

    async fn async_walk_dir<'a>(
        &'a self,
        path: &(dyn AsRef<Path> + Send + Sync),
        is_ignored: IsIgnoredFn,
    ) -> Result<Box<dyn DirectoryWalk + Send + Sync + 'a>, anyhow::Error> {
        struct DdupWalkDir {
            queue: VecDeque<(PathBuf, ddup_bak::archive::entries::Entry)>,
            is_ignored: IsIgnoredFn,
        }

        #[async_trait::async_trait]
        impl DirectoryWalk for DdupWalkDir {
            async fn next_entry(&mut self) -> Option<Result<(FileType, PathBuf), anyhow::Error>> {
                if let Some((path, entry)) = self.queue.pop_front() {
                    let file_type = VirtualDdupBakArchive::ddup_bak_entry_to_file_type(&entry);

                    if let ddup_bak::archive::entries::Entry::Directory(dir) = &entry {
                        for child in &dir.entries {
                            let child_path = path.join(child.name());
                            let child_type =
                                VirtualDdupBakArchive::ddup_bak_entry_to_file_type(child);

                            if (self.is_ignored)(child_type, child_path.clone()).is_some() {
                                self.queue.push_back((child_path, child.clone()));
                            }
                        }
                    }
                    return Some(Ok((file_type, path)));
                }
                None
            }
        }

        let archive = self.archive.clone();
        let path = path.as_ref().to_path_buf();
        let mut queue = VecDeque::new();

        if let Some(entry) = archive.find_archive_entry(&path) {
            if let ddup_bak::archive::entries::Entry::Directory(dir) = entry {
                for child in &dir.entries {
                    let child_path = path.join(child.name());
                    let child_type = Self::ddup_bak_entry_to_file_type(child);
                    if (is_ignored)(child_type, child_path.clone()).is_some() {
                        queue.push_back((child_path, child.clone()));
                    }
                }
            }
        } else if path.components().count() == 0 {
            for child in archive.entries() {
                let child_path = path.join(child.name());
                let child_type = Self::ddup_bak_entry_to_file_type(child);
                if (is_ignored)(child_type, child_path.clone()).is_some() {
                    queue.push_back((child_path, child.clone()));
                }
            }
        }

        Ok(Box::new(DdupWalkDir { queue, is_ignored }))
    }

    async fn async_walk_dir_stream<'a>(
        &'a self,
        path: &(dyn AsRef<Path> + Send + Sync),
        is_ignored: IsIgnoredFn,
    ) -> Result<Box<dyn DirectoryStreamWalk + Send + Sync + 'a>, anyhow::Error> {
        struct DdupStreamWalk {
            repository: Option<Arc<ddup_bak::repository::Repository>>,
            queue: VecDeque<(PathBuf, ddup_bak::archive::entries::Entry)>,
            is_ignored: IsIgnoredFn,
        }

        #[async_trait::async_trait]
        impl DirectoryStreamWalk for DdupStreamWalk {
            async fn next_entry(
                &mut self,
            ) -> Option<Result<(FileType, PathBuf, AsyncReadableFileStream), anyhow::Error>>
            {
                if let Some((path, entry)) = self.queue.pop_front() {
                    let file_type = VirtualDdupBakArchive::ddup_bak_entry_to_file_type(&entry);

                    if let ddup_bak::archive::entries::Entry::Directory(dir) = &entry {
                        for child in &dir.entries {
                            let child_path = path.join(child.name());
                            let child_type =
                                VirtualDdupBakArchive::ddup_bak_entry_to_file_type(child);
                            if (self.is_ignored)(child_type, child_path.clone()).is_some() {
                                self.queue.push_back((child_path, child.clone()));
                            }
                        }
                    }

                    let stream: AsyncReadableFileStream = if entry.is_file() {
                        match self.repository.entry_reader(entry) {
                            Ok(mut entry_reader) => {
                                let (reader, mut writer) = tokio::io::simplex(crate::BUFFER_SIZE);
                                tokio::task::spawn_blocking(move || {
                                    let runtime = tokio::runtime::Handle::current();
                                    let mut buffer = vec![0; crate::BUFFER_SIZE];
                                    loop {
                                        match entry_reader.read(&mut buffer) {
                                            Ok(0) => break,
                                            Ok(n) => {
                                                if runtime
                                                    .block_on(writer.write_all(&buffer[..n]))
                                                    .is_err()
                                                {
                                                    break;
                                                }
                                            }
                                            Err(err) => {
                                                tracing::error!(
                                                    "error reading from ddup_bak entry: {:?}",
                                                    err
                                                );
                                                break;
                                            }
                                        }
                                    }

                                    runtime.block_on(writer.shutdown()).ok();
                                });
                                Box::new(reader)
                            }
                            Err(err) => return Some(Err(err)),
                        }
                    } else {
                        Box::new(tokio::io::empty())
                    };

                    return Some(Ok((file_type, path, stream)));
                }
                None
            }
        }

        let archive = self.archive.clone();
        let path = path.as_ref().to_path_buf();
        let mut queue = VecDeque::new();

        if let Some(entry) = archive.find_archive_entry(&path) {
            if let ddup_bak::archive::entries::Entry::Directory(dir) = entry {
                for child in &dir.entries {
                    let child_path = path.join(child.name());
                    let child_type = Self::ddup_bak_entry_to_file_type(child);
                    if (is_ignored)(child_type, child_path.clone()).is_some() {
                        queue.push_back((child_path, child.clone()));
                    }
                }
            }
        } else if path.components().count() == 0 {
            for child in archive.entries() {
                let child_path = path.join(child.name());
                let child_type = Self::ddup_bak_entry_to_file_type(child);
                if (is_ignored)(child_type, child_path.clone()).is_some() {
                    queue.push_back((child_path, child.clone()));
                }
            }
        }

        Ok(Box::new(DdupStreamWalk {
            repository: self.repository.clone(),
            queue,
            is_ignored,
        }))
    }

    async fn close(&self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}
