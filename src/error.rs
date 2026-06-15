use snafu::{Location, Snafu, location};
use std::{num::ParseIntError, str::Utf8Error};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("unexpected, err: {}", err_msg))]
    Unexpected {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("{msg}"))]
    PlainText {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("io error, {}", reason))]
    Io {
        source: std::io::Error,
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("invalid utf-8 in lsp headers: {source}"))]
    InvalidHeaderUtf8 {
        source: Utf8Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("invalid Content-Length value"))]
    InvalidContentLength {
        source: ParseIntError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("missing Content-Length header"))]
    MissingContentLength {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("invalid json"))]
    InvalidJson {
        source: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        Self::Io {
            source,
            reason: "".to_string(),
            location: location!(),
        }
    }
}
