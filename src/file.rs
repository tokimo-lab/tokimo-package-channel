use bytes::Bytes;

use crate::error::ChannelError;

/// Describes a file to send via a channel driver.
#[derive(Debug, Clone)]
pub enum FilePayload {
    /// File data provided as raw bytes.
    Bytes {
        data: Bytes,
        filename: String,
        /// Optional MIME type. When absent, drivers may attempt to detect
        /// from the filename extension or default to `application/octet-stream`.
        content_type: Option<String>,
    },
    /// File referenced by URL. Some platforms (QQ Bot, Telegram) accept URLs
    /// directly; others will download first.
    Url(String),
}

impl FilePayload {
    /// Returns the filename if available.
    pub fn filename(&self) -> Option<&str> {
        match self {
            Self::Bytes { filename, .. } => Some(filename),
            Self::Url(url) => url.rsplit('/').next().filter(|s| !s.is_empty()),
        }
    }

    /// Returns the content type hint if available.
    pub fn content_type_hint(&self) -> Option<&str> {
        match self {
            Self::Bytes { content_type, .. } => content_type.as_deref(),
            Self::Url(url) => {
                let fname = url.rsplit('/').next().unwrap_or("");
                Some(guess_content_type(fname))
            }
        }
    }
}

/// Guess MIME type from a filename extension.
pub fn guess_content_type(filename: &str) -> &'static str {
    let ext = filename.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
    match ext.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "bmp" => "image/bmp",
        "svg" => "image/svg+xml",
        "tiff" | "tif" => "image/tiff",
        "ico" => "image/x-icon",
        "pdf" => "application/pdf",
        "doc" => "application/msword",
        "docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "xls" => "application/vnd.ms-excel",
        "xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "ppt" => "application/vnd.ms-powerpoint",
        "pptx" => "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "zip" => "application/zip",
        "gz" | "gzip" => "application/gzip",
        "tar" => "application/x-tar",
        "mp3" => "audio/mpeg",
        "ogg" => "audio/ogg",
        "wav" => "audio/wav",
        "mp4" => "video/mp4",
        "mov" => "video/quicktime",
        "avi" => "video/x-msvideo",
        "mkv" => "video/x-matroska",
        "txt" => "text/plain",
        "html" | "htm" => "text/html",
        "css" => "text/css",
        "js" => "application/javascript",
        "json" => "application/json",
        "xml" => "application/xml",
        "csv" => "text/csv",
        "md" => "text/markdown",
        _ => "application/octet-stream",
    }
}

/// Returns `true` if the content type (or filename) looks like an image.
pub fn is_image_content_type(content_type: &str) -> bool {
    content_type.starts_with("image/")
}

/// Resolve a [`FilePayload`] to raw bytes, downloading from URL if needed.
///
/// Returns `(data, filename, content_type)`.
pub async fn resolve_to_bytes(
    client: &reqwest::Client,
    file: &FilePayload,
) -> Result<(Bytes, String, Option<String>), ChannelError> {
    match file {
        FilePayload::Bytes {
            data,
            filename,
            content_type,
        } => Ok((data.clone(), filename.clone(), content_type.clone())),
        FilePayload::Url(url) => {
            let resp = client.get(url).send().await?;
            let content_type = resp
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            let filename = url
                .rsplit('/')
                .next()
                .filter(|s| !s.is_empty())
                .unwrap_or("file")
                .to_string();
            let data = resp.bytes().await?;
            Ok((data, filename, content_type))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_guess_content_type() {
        assert_eq!(guess_content_type("photo.png"), "image/png");
        assert_eq!(guess_content_type("doc.pdf"), "application/pdf");
        assert_eq!(guess_content_type("video.mp4"), "video/mp4");
        assert_eq!(guess_content_type("unknown.xyz"), "application/octet-stream");
    }

    #[test]
    fn test_is_image_content_type() {
        assert!(is_image_content_type("image/png"));
        assert!(is_image_content_type("image/jpeg"));
        assert!(!is_image_content_type("application/pdf"));
        assert!(!is_image_content_type("text/plain"));
    }

    #[test]
    fn test_file_payload_filename() {
        let bytes_payload = FilePayload::Bytes {
            data: Bytes::new(),
            filename: "test.png".into(),
            content_type: None,
        };
        assert_eq!(bytes_payload.filename(), Some("test.png"));

        let url_payload = FilePayload::Url("https://example.com/path/doc.pdf".into());
        assert_eq!(url_payload.filename(), Some("doc.pdf"));
    }
}
