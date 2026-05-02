use std::{
    io,
    pin::Pin,
    str,
    task::{Context, Poll},
};

use serde_json::Value;
use tokio::io::{AsyncRead, ReadBuf};
use tokio_stream::Stream;

pub type ClientId = u32;

const HEADER_DELIMITER: &[u8] = b"\r\n\r\n";

#[derive(Debug, Clone)]
pub struct LspPacket {
    pub body: Value,
}

impl LspPacket {
    pub fn from_body(body: Value) -> Self {
        Self { body }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let body = serde_json::to_vec(&self.body).unwrap_or_else(|_| b"null".to_vec());
        let mut out = Vec::with_capacity(body.len() + 32);
        out.extend_from_slice(format!("Content-Length: {}\r\n\r\n", body.len()).as_bytes());
        out.extend_from_slice(&body);
        out
    }

    pub fn parse_json(&self) -> Option<Value> {
        Some(self.body.clone())
    }

    pub fn is_method(&self, target: &str) -> bool {
        self.parse_json()
            .and_then(|json| {
                json.get("method")
                    .and_then(serde_json::Value::as_str)
                    .map(|method| method == target)
            })
            .unwrap_or(false)
    }

    pub fn is_request_method(&self, target: &str) -> bool {
        self.parse_json()
            .and_then(|json| {
                let method = json.get("method")?.as_str()?;
                let has_id = json.get("id").is_some();
                Some(method == target && has_id)
            })
            .unwrap_or(false)
    }
}

#[derive(Default, Debug)]
pub struct LspPacketDecoder {
    buf: Vec<u8>,
}

impl LspPacketDecoder {
    pub fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    pub fn next_packet(&mut self) -> io::Result<Option<LspPacket>> {
        let Some(header_end) = find_header_end(&self.buf) else {
            return Ok(None);
        };

        let content_len = parse_content_length(&self.buf[..header_end])?;
        let body_start = header_end + HEADER_DELIMITER.len();
        let total_len = body_start + content_len;

        if self.buf.len() < total_len {
            return Ok(None);
        }

        let body_bytes = self.buf[body_start..total_len].to_vec();
        self.buf.drain(..total_len);
        let body = serde_json::from_slice(&body_bytes)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
        Ok(Some(LspPacket::from_body(body)))
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn clear(&mut self) {
        self.buf.clear();
    }
}

pub struct LspPacketStream<R> {
    reader: R,
    decoder: LspPacketDecoder,
    read_buf: Vec<u8>,
    terminated: bool,
}

impl<R> LspPacketStream<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            decoder: LspPacketDecoder::default(),
            read_buf: vec![0; 8192],
            terminated: false,
        }
    }
}

impl<R> Stream for LspPacketStream<R>
where
    R: AsyncRead + Unpin,
{
    type Item = io::Result<LspPacket>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.decoder.next_packet() {
                Ok(Some(packet)) => return Poll::Ready(Some(Ok(packet))),
                Ok(None) => {}
                Err(err) => {
                    self.decoder.clear();
                    self.terminated = true;
                    return Poll::Ready(Some(Err(err)));
                }
            }

            if self.terminated {
                return Poll::Ready(None);
            }

            let filled = {
                let this = &mut *self;
                let mut read_buf = ReadBuf::new(&mut this.read_buf);
                match Pin::new(&mut this.reader).poll_read(cx, &mut read_buf) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => read_buf.filled().len(),
                    Poll::Ready(Err(err)) => {
                        self.terminated = true;
                        return Poll::Ready(Some(Err(err)));
                    }
                }
            };

            if filled == 0 {
                self.terminated = true;
                if self.decoder.is_empty() {
                    return Poll::Ready(None);
                }

                self.decoder.clear();
                return Poll::Ready(Some(Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected eof while reading lsp packet",
                ))));
            }

            {
                let this = &mut *self;
                let decoder = &mut this.decoder;
                let read_buf = &this.read_buf;
                decoder.push(&read_buf[..filled]);
            }
        }
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(HEADER_DELIMITER.len())
        .position(|window| window == HEADER_DELIMITER)
}

fn parse_content_length(headers: &[u8]) -> io::Result<usize> {
    let headers = str::from_utf8(headers)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;

    for line in headers.split("\r\n") {
        let (name, value) = match line.split_once(':') {
            Some(parts) => parts,
            None => continue,
        };

        if name.trim().eq_ignore_ascii_case("content-length") {
            let len = value.trim().parse::<usize>().map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid Content-Length: {err}"),
                )
            })?;
            return Ok(len);
        }
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "missing Content-Length header",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::StreamExt;

    #[test]
    fn decoder_handles_split_headers_and_body() {
        let body = br#"{"jsonrpc":"2.0"}"#;
        let expected: Value = serde_json::from_slice(body).unwrap();
        let mut decoder = LspPacketDecoder::default();
        decoder.push(format!("Content-Length: {}\r\n", body.len()).as_bytes());
        assert!(decoder.next_packet().unwrap().is_none());

        decoder.push(b"\r\n");
        decoder.push(body);
        let packet = decoder.next_packet().unwrap().expect("packet should exist");
        assert_eq!(packet.body, expected);
    }

    #[tokio::test]
    async fn packet_stream_reads_split_packet() {
        let body = br#"{"jsonrpc":"2.0"}"#;
        let expected: Value = serde_json::from_slice(body).unwrap();
        let bytes = format!("Content-Length: {}\r\n\r\n", body.len()).into_bytes();
        let mut payload = bytes;
        payload.extend_from_slice(body);
        let reader = tokio::io::duplex(64);
        let (mut tx, rx) = reader;
        let writer = tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;

            tx.write_all(&payload[..20]).await.unwrap();
            tx.write_all(&payload[20..]).await.unwrap();
        });

        let mut stream = LspPacketStream::new(rx);
        let packet = stream
            .next()
            .await
            .expect("packet should exist")
            .expect("packet should decode");
        assert_eq!(packet.body, expected);

        writer.await.unwrap();
    }
}
