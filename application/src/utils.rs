use crate::routes::MimeCacheValue;
use std::{
    path::{Path, PathBuf},
    sync::LazyLock,
};

pub fn draw_progress_bar(width: usize, current: f64, total: f64) -> String {
    let progress_percentage = (current / total) * 100.0;
    let formatted_percentage = if progress_percentage.is_finite() {
        &format!("{:.2}%", progress_percentage)
    } else {
        "0.00%"
    };

    let completed_width = std::cmp::min(
        (progress_percentage / 100.0 * width as f64).round() as usize,
        width,
    );
    let remaining_width = width - completed_width;

    let bar = if completed_width == width {
        "=".repeat(width)
    } else {
        format!(
            "{}{}{}",
            "=".repeat(completed_width),
            ">",
            " ".repeat(remaining_width.saturating_sub(1))
        )
    };

    format!("[{bar}] {formatted_percentage}")
}

#[inline]
pub fn slice_after_question_mark(s: &str) -> &str {
    s.split_once('?').map(|(_, after)| after).unwrap_or("")
}

pub fn parse_content_disposition_filename(header: &str) -> Option<String> {
    static RE_STAR: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"(?i)filename\*=utf-8''([^;]+)").unwrap());

    if let Some(caps) = RE_STAR.captures(header) {
        let encoded_filename = &caps[1];

        if let Ok(decoded) = percent_encoding::percent_decode_str(encoded_filename).decode_utf8() {
            return Some(slice_after_question_mark(&decoded).to_string());
        }
    }

    static RE_LEGACY: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r#"(?i)filename="?([^";]+)"?"#).unwrap());

    if let Some(caps) = RE_LEGACY.captures(header) {
        return Some(slice_after_question_mark(&caps[1]).to_string());
    }

    None
}

pub fn detect_inner_utf8(path: &Path, mime: &str) -> bool {
    let compression_type = crate::io::compression::CompressionType::from_mime(mime);

    if matches!(
        compression_type,
        crate::io::compression::CompressionType::None
    ) {
        return false;
    }

    let Some(file_stem) = path.file_stem().and_then(|s| s.to_str()) else {
        return false;
    };

    if let Some(stem_mime) = new_mime_guess::from_path(file_stem).first_raw() {
        const ADDITIONAL_TEXT_MIME_TYPES: &[&str] = &[
            "application/json",
            "application/javascript",
            "application/xml",
            "application/x-yaml",
            "application/yaml",
            "application/toml",
            "application/sql",
            "application/x-sh",
            "application/x-httpd-php",
            "image/svg+xml",
        ];

        stem_mime.starts_with("text/") || ADDITIONAL_TEXT_MIME_TYPES.contains(&stem_mime)
    } else {
        false
    }
}

pub fn detect_mime_type(path: &Path, buffer: Option<&[u8]>) -> MimeCacheValue {
    let valid_utf8 = buffer.is_some_and(|buffer| is_valid_utf8_slice(buffer) || buffer.is_empty());

    if let Some(buffer) = buffer
        && let Some(mime) = infer::get(buffer)
    {
        MimeCacheValue {
            mime: mime.mime_type(),
            valid_utf8,
            valid_inner_utf8: detect_inner_utf8(path, mime.mime_type()),
        }
    } else if let Some(mime) = new_mime_guess::from_path(path).first_raw() {
        MimeCacheValue {
            mime,
            valid_utf8,
            valid_inner_utf8: detect_inner_utf8(path, mime),
        }
    } else if valid_utf8 {
        MimeCacheValue {
            mime: "text/plain",
            valid_utf8: true,
            valid_inner_utf8: false,
        }
    } else {
        MimeCacheValue {
            mime: "application/octet-stream",
            valid_utf8: false,
            valid_inner_utf8: false,
        }
    }
}

pub fn deduplicate_paths(mut paths: Vec<PathBuf>) -> Vec<PathBuf> {
    if paths.is_empty() {
        return Vec::new();
    }

    paths.sort();
    paths.dedup();

    let mut unique = Vec::new();
    for path in paths {
        if let Some(last) = unique.last()
            && path.starts_with(last)
        {
            continue;
        }

        unique.push(path);
    }

    unique
}

#[inline]
pub fn is_valid_utf8_slice(s: &[u8]) -> bool {
    let mut idx = s.len();
    while idx > s.len().saturating_sub(4) {
        if str::from_utf8(&s[..idx]).is_ok() {
            return true;
        }

        idx -= 1;
    }

    false
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PortablePermissions {
    pub mode: u32,
}

impl PortablePermissions {
    pub fn from_mode(mode: u32) -> Self {
        Self { mode }
    }

    pub fn is_readonly(&self) -> bool {
        self.mode & 0o200 == 0
    }

    pub fn into_std_permissions(self) -> Option<std::fs::Permissions> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            Some(std::fs::Permissions::from_mode(self.mode))
        }
        #[cfg(windows)]
        None
    }
}

#[cfg(unix)]
impl From<std::fs::Permissions> for PortablePermissions {
    fn from(perms: std::fs::Permissions) -> Self {
        Self {
            mode: std::os::unix::fs::PermissionsExt::mode(&perms),
        }
    }
}

#[cfg(unix)]
impl From<cap_std::fs::Permissions> for PortablePermissions {
    fn from(perms: cap_std::fs::Permissions) -> Self {
        Self {
            mode: cap_std::fs::PermissionsExt::mode(&perms),
        }
    }
}

#[cfg(not(unix))]
impl From<std::fs::Permissions> for PortablePermissions {
    fn from(perms: std::fs::Permissions) -> Self {
        Self {
            mode: if perms.readonly() { 0o444 } else { 0o666 },
        }
    }
}

#[cfg(not(unix))]
impl From<cap_std::fs::Permissions> for PortablePermissions {
    fn from(perms: cap_std::fs::Permissions) -> Self {
        Self {
            mode: if perms.readonly() { 0o444 } else { 0o666 },
        }
    }
}

pub trait PortablePermissionsApplier {
    fn apply_permissions(&self, new_permissions: PortablePermissions) -> std::io::Result<()>;
}

#[cfg(unix)]
impl PortablePermissionsApplier for std::fs::File {
    fn apply_permissions(&self, new_permissions: PortablePermissions) -> std::io::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let permissions = std::fs::Permissions::from_mode(new_permissions.mode);
        self.set_permissions(permissions)
    }
}

#[cfg(unix)]
impl PortablePermissionsApplier for cap_std::fs::File {
    fn apply_permissions(&self, new_permissions: PortablePermissions) -> std::io::Result<()> {
        use cap_std::fs::PermissionsExt;

        let permissions = cap_std::fs::Permissions::from_mode(new_permissions.mode);
        self.set_permissions(permissions)
    }
}

#[cfg(not(unix))]
impl PortablePermissionsApplier for std::fs::File {
    fn apply_permissions(&self, new_permissions: PortablePermissions) -> std::io::Result<()> {
        let mut permissions = self.metadata()?.permissions();
        permissions.set_readonly(new_permissions.is_readonly());
        self.set_permissions(permissions)
    }
}

#[cfg(not(unix))]
impl PortablePermissionsApplier for cap_std::fs::File {
    fn apply_permissions(&self, new_permissions: PortablePermissions) -> std::io::Result<()> {
        let mut permissions = self.metadata()?.permissions();
        permissions.set_readonly(new_permissions.is_readonly());
        self.set_permissions(permissions)
    }
}

pub trait PortableSizeExt {
    fn size_logical(&self) -> u64;
    fn size_physical(&self) -> u64;
}

#[cfg(unix)]
impl PortableSizeExt for std::fs::Metadata {
    fn size_logical(&self) -> u64 {
        self.len()
    }

    fn size_physical(&self) -> u64 {
        std::os::unix::fs::MetadataExt::blocks(self) * 512
    }
}

#[cfg(unix)]
impl PortableSizeExt for cap_std::fs::Metadata {
    fn size_logical(&self) -> u64 {
        self.len()
    }

    fn size_physical(&self) -> u64 {
        cap_std::fs::MetadataExt::blocks(self) * 512
    }
}

#[cfg(not(unix))]
impl PortableSizeExt for std::fs::Metadata {
    fn size_logical(&self) -> u64 {
        self.len()
    }

    fn size_physical(&self) -> u64 {
        self.len()
    }
}

#[cfg(not(unix))]
impl PortableSizeExt for cap_std::fs::Metadata {
    fn size_logical(&self) -> u64 {
        self.len()
    }

    fn size_physical(&self) -> u64 {
        self.len()
    }
}
