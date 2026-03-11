use std::convert::Infallible;

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{Response, StatusCode};

pub type BoxBody = http_body_util::combinators::BoxBody<Bytes, Infallible>;

pub enum PipeError {
    EmptyKey,
    MethodNotAllowed,
    KeyAlreadyExists,
    KeyNotFound,
    Draining,
    TooManyOpenFiles,
    DiskFull,
    DiskQuotaExceeded,
    IoError(std::io::Error),
    UploadError(hyper::Error),
}

impl PipeError {
    pub fn from_io(e: std::io::Error) -> Self {
        match e.raw_os_error() {
            Some(libc::EMFILE | libc::ENFILE) => Self::TooManyOpenFiles,
            Some(libc::ENOSPC) => Self::DiskFull,
            _ => Self::IoError(e),
        }
    }

    fn status(&self) -> StatusCode {
        match self {
            Self::EmptyKey => StatusCode::BAD_REQUEST,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::KeyAlreadyExists => StatusCode::CONFLICT,
            Self::KeyNotFound => StatusCode::NOT_FOUND,
            Self::Draining | Self::TooManyOpenFiles | Self::DiskFull | Self::DiskQuotaExceeded => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            Self::IoError(_) | Self::UploadError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn message(&self) -> String {
        match self {
            Self::EmptyKey => "missing key in path".into(),
            Self::MethodNotAllowed => "only PUT and GET are supported".into(),
            Self::KeyAlreadyExists => "key already exists".into(),
            Self::KeyNotFound => "key not found".into(),
            Self::Draining => "server is shutting down, not accepting new uploads".into(),
            Self::TooManyOpenFiles => "too many open files, try again later".into(),
            Self::DiskFull => "disk full, try again later".into(),
            Self::DiskQuotaExceeded => "disk quota exceeded, try again later".into(),
            Self::IoError(e) => format!("internal error: {e}"),
            Self::UploadError(e) => format!("upload failed: {e}"),
        }
    }

    pub fn into_response(self) -> Response<BoxBody> {
        let body = self.message();
        eprintln!("[ERROR] {} {}", self.status().as_u16(), body);

        Response::builder()
            .status(self.status())
            .header(hyper::header::CONTENT_TYPE, "text/plain")
            .body(
                http_body_util::Full::new(Bytes::from(body))
                    .map_err(|never| match never {})
                    .boxed(),
            )
            .unwrap()
    }
}

pub fn ok_response() -> Response<BoxBody> {
    Response::builder()
        .status(StatusCode::OK)
        .body(
            http_body_util::Empty::new()
                .map_err(|never| match never {})
                .boxed(),
        )
        .unwrap()
}
