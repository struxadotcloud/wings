use crate::{
    io::{
        abort::{AbortGuard, AbortWriter},
        compression::{CompressionLevel, CompressionType, writer::CompressionWriter},
        counting_reader::CountingReader,
        fixed_reader::FixedReader,
    },
    server::filesystem::virtualfs::IsIgnoredFn,
    utils::PortablePermissions,
};
use itaf::encoder::{EncoderOptions, ItafEncoder, Metadata};
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

pub struct CreateItafOptions {
    pub compression_type: CompressionType,
    pub compression_level: CompressionLevel,
    pub threads: usize,
    pub crc_enabled: bool,
}

fn itaf_metadata(metadata: &cap_std::fs::Metadata) -> Metadata {
    Metadata {
        uid: 0,
        gid: 0,
        mode: PortablePermissions::from(metadata.permissions()).mode,
        modified: metadata
            .modified()
            .map(|t| t.into_std())
            .unwrap_or_else(|_| std::time::SystemTime::now()),
    }
}

pub async fn create_itaf<W: Write + Send + 'static>(
    filesystem: crate::server::filesystem::cap::CapFilesystem,
    destination: W,
    base: &Path,
    sources: Vec<impl AsRef<Path> + Send + 'static>,
    bytes_archived: Option<Arc<AtomicU64>>,
    is_ignored: IsIgnoredFn,
    options: CreateItafOptions,
) -> Result<W, anyhow::Error> {
    let base = filesystem.relative_path(base);
    let (_guard, listener) = AbortGuard::new();

    tokio::task::spawn_blocking(move || {
        let writer = CompressionWriter::new(
            destination,
            options.compression_type,
            options.compression_level,
            options.threads,
        )?;
        let writer = AbortWriter::new(writer, listener);
        let mut archive = ItafEncoder::new(
            writer,
            EncoderOptions {
                base_timestamp: None,
                crc_enabled: options.crc_enabled,
            },
        )?;

        for source in sources {
            let relative = source.as_ref();
            let source = base.join(relative);

            let source_metadata = match filesystem.symlink_metadata(&source) {
                Ok(metadata) => metadata,
                Err(_) => continue,
            };

            let Some(source) = (is_ignored)(source_metadata.file_type().into(), source) else {
                continue;
            };

            let meta = itaf_metadata(&source_metadata);

            if source_metadata.is_dir() {
                let components = path_components(relative);
                enter_path_components(&mut archive, &components, &meta)?;

                let mut walker = filesystem
                    .walk_dir(source)?
                    .with_is_ignored(is_ignored.clone());

                let mut dir_stack = components.clone();

                while let Some(Ok((_, path))) = walker.next_entry() {
                    let rel = match path.strip_prefix(&base) {
                        Ok(r) => r,
                        Err(_) => continue,
                    };

                    let metadata = match filesystem.symlink_metadata(&path) {
                        Ok(m) => m,
                        Err(_) => continue,
                    };

                    let entry_components = path_components(rel);
                    let entry_dirs = &entry_components[..entry_components.len().saturating_sub(1)];
                    let entry_name = match entry_components.last() {
                        Some(n) => n.clone(),
                        None => continue,
                    };

                    sync_dir_stack(&mut archive, &mut dir_stack, entry_dirs)?;

                    let entry_meta = itaf_metadata(&metadata);

                    if metadata.is_dir() {
                        archive.enter_dir(&entry_name, &entry_meta)?;
                        dir_stack.push(entry_name);

                        if let Some(bytes_archived) = &bytes_archived {
                            bytes_archived.fetch_add(metadata.len(), Ordering::SeqCst);
                        }
                    } else if metadata.is_file() {
                        let file = filesystem.open(&path)?;
                        let reader: Box<dyn Read> = match &bytes_archived {
                            Some(bytes_archived) => Box::new(CountingReader::new_with_bytes_read(
                                file,
                                Arc::clone(bytes_archived),
                            )),
                            None => Box::new(file),
                        };
                        let reader =
                            FixedReader::new_with_fixed_bytes(reader, metadata.len() as usize);

                        archive
                            .add_file(&entry_name, &entry_meta, metadata.len(), &mut { reader })?;
                    } else if let Ok(link_target) = filesystem.read_link_contents(&path) {
                        let target = link_target.to_string_lossy();

                        if itaf::spec::validate_name(&entry_name).is_ok() {
                            archive.add_symlink(
                                &entry_name,
                                &target,
                                metadata.is_dir(),
                                &entry_meta,
                            )?;
                        }
                    }
                }

                let target_depth = 0;
                while dir_stack.len() > target_depth {
                    archive.exit_dir()?;
                    dir_stack.pop();
                }

                if let Some(bytes_archived) = &bytes_archived {
                    bytes_archived.fetch_add(source_metadata.len(), Ordering::SeqCst);
                }
            } else if source_metadata.is_file() {
                let components = path_components(relative);
                let name = match components.last() {
                    Some(n) => n.clone(),
                    None => continue,
                };
                let enclosing = components[..components.len() - 1].to_vec();

                enter_path_components(&mut archive, &enclosing, &meta)?;

                let file = filesystem.open(&source)?;
                let reader: Box<dyn Read> = match &bytes_archived {
                    Some(bytes_archived) => Box::new(CountingReader::new_with_bytes_read(
                        file,
                        Arc::clone(bytes_archived),
                    )),
                    None => Box::new(file),
                };
                let reader =
                    FixedReader::new_with_fixed_bytes(reader, source_metadata.len() as usize);
                let reader = std::io::BufReader::with_capacity(crate::BUFFER_SIZE, reader);

                archive.add_file(&name, &meta, source_metadata.len(), &mut { reader })?;

                exit_path_components(&mut archive, enclosing.len())?;
            } else if let Ok(link_target) = filesystem.read_link_contents(&source) {
                let components = path_components(relative);
                let name = match components.last() {
                    Some(n) => n.clone(),
                    None => continue,
                };
                let enclosing = components[..components.len() - 1].to_vec();

                enter_path_components(&mut archive, &enclosing, &meta)?;

                let target = link_target.to_string_lossy();
                if itaf::spec::validate_name(&name).is_ok() {
                    archive.add_symlink(&name, &target, source_metadata.is_dir(), &meta)?;
                }

                exit_path_components(&mut archive, enclosing.len())?;

                if let Some(bytes_archived) = &bytes_archived {
                    bytes_archived.fetch_add(source_metadata.len(), Ordering::SeqCst);
                }
            }
        }

        let mut inner = archive.finish()?.into_inner().finish()?;
        inner.flush()?;

        Ok(inner)
    })
    .await?
}

pub async fn create_itaf_distributed<W: Write + Send + 'static>(
    filesystem: crate::server::filesystem::cap::CapFilesystem,
    destination: W,
    base: &Path,
    sources: async_channel::Receiver<PathBuf>,
    bytes_archived: Option<Arc<AtomicU64>>,
    options: CreateItafOptions,
) -> Result<W, anyhow::Error> {
    let base = filesystem.relative_path(base);
    let (_guard, listener) = AbortGuard::new();

    tokio::task::spawn_blocking(move || {
        let writer = CompressionWriter::new(
            destination,
            options.compression_type,
            options.compression_level,
            options.threads,
        )?;
        let writer = AbortWriter::new(writer, listener);
        let mut archive = ItafEncoder::new(
            writer,
            EncoderOptions {
                base_timestamp: None,
                crc_enabled: options.crc_enabled,
            },
        )?;

        let mut dir_stack = Vec::new();

        while let Ok(source) = sources.recv_blocking() {
            let relative = &source;
            let full = base.join(relative);

            let metadata = match filesystem.symlink_metadata(&full) {
                Ok(m) => m,
                Err(_) => continue,
            };

            let meta = itaf_metadata(&metadata);

            let components = path_components(relative);
            if components.is_empty() {
                continue;
            }

            if metadata.is_dir() {
                let parent_components = &components[..components.len() - 1];
                sync_dir_stack_with_meta(
                    &mut archive,
                    &mut dir_stack,
                    parent_components,
                    &filesystem,
                    &base,
                )?;

                let name = components.last().unwrap();
                archive.enter_dir(name, &meta)?;
                dir_stack.push(name.clone());

                if let Some(bytes_archived) = &bytes_archived {
                    bytes_archived.fetch_add(metadata.len(), Ordering::SeqCst);
                }
            } else if metadata.is_file() {
                let dir_components = &components[..components.len() - 1];
                sync_dir_stack_with_meta(
                    &mut archive,
                    &mut dir_stack,
                    dir_components,
                    &filesystem,
                    &base,
                )?;

                let name = components.last().unwrap();

                let file = filesystem.open(&full)?;
                let reader: Box<dyn Read> = match &bytes_archived {
                    Some(bytes_archived) => Box::new(CountingReader::new_with_bytes_read(
                        file,
                        Arc::clone(bytes_archived),
                    )),
                    None => Box::new(file),
                };
                let reader = FixedReader::new_with_fixed_bytes(reader, metadata.len() as usize);
                let reader = std::io::BufReader::with_capacity(crate::TRANSFER_BUFFER_SIZE, reader);

                archive.add_file(name, &meta, metadata.len(), &mut { reader })?;
            } else if let Ok(link_target) = filesystem.read_link_contents(&full) {
                let dir_components = &components[..components.len() - 1];
                sync_dir_stack_with_meta(
                    &mut archive,
                    &mut dir_stack,
                    dir_components,
                    &filesystem,
                    &base,
                )?;

                let name = components.last().unwrap();
                let target = link_target.to_string_lossy();
                if itaf::spec::validate_name(name).is_ok() {
                    archive.add_symlink(name, &target, metadata.is_dir(), &meta)?;
                }

                if let Some(bytes_archived) = &bytes_archived {
                    bytes_archived.fetch_add(metadata.len(), Ordering::SeqCst);
                }
            }
        }

        exit_path_components(&mut archive, dir_stack.len())?;

        let mut inner = archive.finish()?.into_inner().finish()?;
        inner.flush()?;

        Ok(inner)
    })
    .await?
}

fn path_components(path: &Path) -> Vec<compact_str::CompactString> {
    path.components()
        .filter_map(|c| match c {
            std::path::Component::Normal(s) => Some(s.to_string_lossy().into()),
            _ => None,
        })
        .collect()
}

fn enter_path_components<W: Write>(
    archive: &mut ItafEncoder<W>,
    components: &[compact_str::CompactString],
    meta: &Metadata,
) -> Result<(), std::io::Error> {
    for component in components {
        archive.enter_dir(component, meta)?;
    }
    Ok(())
}

fn exit_path_components<W: Write>(
    archive: &mut ItafEncoder<W>,
    count: usize,
) -> Result<(), std::io::Error> {
    for _ in 0..count {
        archive.exit_dir()?;
    }
    Ok(())
}

fn sync_dir_stack<W: Write>(
    archive: &mut ItafEncoder<W>,
    dir_stack: &mut Vec<compact_str::CompactString>,
    target: &[compact_str::CompactString],
) -> Result<(), std::io::Error> {
    let shared = dir_stack
        .iter()
        .zip(target.iter())
        .take_while(|(a, b)| a == b)
        .count();

    while dir_stack.len() > shared {
        archive.exit_dir()?;
        dir_stack.pop();
    }

    for component in &target[shared..] {
        let meta = Metadata {
            uid: 0,
            gid: 0,
            mode: 0o755,
            modified: std::time::SystemTime::now(),
        };
        archive.enter_dir(component, &meta)?;
        dir_stack.push(component.clone());
    }

    Ok(())
}

fn sync_dir_stack_with_meta<W: Write>(
    archive: &mut ItafEncoder<W>,
    dir_stack: &mut Vec<compact_str::CompactString>,
    target: &[compact_str::CompactString],
    filesystem: &crate::server::filesystem::cap::CapFilesystem,
    base: &Path,
) -> Result<(), std::io::Error> {
    let shared = dir_stack
        .iter()
        .zip(target.iter())
        .take_while(|(a, b)| a == b)
        .count();

    while dir_stack.len() > shared {
        archive.exit_dir()?;
        dir_stack.pop();
    }

    for component in &target[shared..] {
        dir_stack.push(component.clone());

        let mut dir_path = base.to_path_buf();
        for seg in dir_stack.iter() {
            dir_path.push(seg);
        }

        let meta = match filesystem.symlink_metadata(&dir_path) {
            Ok(m) => itaf_metadata(&m),
            Err(_) => Metadata {
                uid: 0,
                gid: 0,
                mode: 0o755,
                modified: std::time::SystemTime::now(),
            },
        };

        archive.enter_dir(component, &meta)?;
    }

    Ok(())
}
