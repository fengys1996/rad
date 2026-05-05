use snafu::{Location, Snafu};
use std::{num::ParseIntError, str::Utf8Error};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(transparent)]
    Io {
        source: std::io::Error,
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
