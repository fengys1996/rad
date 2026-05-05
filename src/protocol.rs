use std::io;

use bytes::BytesMut;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tokio_stream::Stream;

pub mod control;
pub mod lsp;

pub use control::{
    ControlFrame, MAGIC, TYPE_STATUS_REQ, TYPE_STATUS_RESP, decode_frame, encode_frame,
    encode_status_req, encode_status_resp,
};
pub use lsp::{ClientId, LspFrame, LspFrameDecoder, LspFrameStream};

pub enum RadMessage {
    Lsp(LspFrame),
    Control(ControlFrame),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum StreamMode {
    Lsp,
    Control,
}

pub struct RadMessageStream<R> {
    reader: R,
    mode: Option<StreamMode>,
    raw_buf: Vec<u8>,
    lsp_decoder: LspFrameDecoder,
    lsp_buf: BytesMut,
    read_buf: Vec<u8>,
    terminated: bool,
}

impl<R> RadMessageStream<R>
where
    R: AsyncRead + Unpin,
{
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            mode: None,
            raw_buf: Vec::with_capacity(8192),
            lsp_decoder: LspFrameDecoder,
            lsp_buf: BytesMut::with_capacity(8192),
            read_buf: vec![0; 8192],
            terminated: false,
        }
    }
}

impl<R> Stream for RadMessageStream<R>
where
    R: AsyncRead + Unpin,
{
    type Item = io::Result<RadMessage>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.mode.is_none() && self.raw_buf.len() >= 4 {
                self.mode = Some(
                    if u32::from_be_bytes([
                        self.raw_buf[0],
                        self.raw_buf[1],
                        self.raw_buf[2],
                        self.raw_buf[3],
                    ]) == MAGIC
                    {
                        StreamMode::Control
                    } else {
                        StreamMode::Lsp
                    },
                );
            }

            match self.mode {
                Some(StreamMode::Control) if self.raw_buf.len() >= 13 => {
                    let payload_len = u32::from_be_bytes([
                        self.raw_buf[9],
                        self.raw_buf[10],
                        self.raw_buf[11],
                        self.raw_buf[12],
                    ]) as usize;
                    let total = 13 + payload_len;
                    if self.raw_buf.len() >= total {
                        let frame_bytes = self.raw_buf.drain(..total).collect::<Vec<_>>();
                        self.mode = None;
                        return Poll::Ready(Some(
                            decode_frame(&frame_bytes).map(RadMessage::Control),
                        ));
                    }
                }
                Some(StreamMode::Control) => {}
                Some(StreamMode::Lsp) => {
                    if !self.raw_buf.is_empty() {
                        let pending = std::mem::take(&mut self.raw_buf);
                        self.lsp_buf.extend_from_slice(&pending);
                    }
                    let decode_result = {
                        let this = &mut *self;
                        let decoder = &mut this.lsp_decoder;
                        let lsp_buf = &mut this.lsp_buf;
                        tokio_util::codec::Decoder::decode(decoder, lsp_buf)
                    };
                    match decode_result {
                        Ok(Some(packet)) => return Poll::Ready(Some(Ok(RadMessage::Lsp(packet)))),
                        Ok(None) => {}
                        Err(_err) => {
                            todo!()
                            // FIXME
                            // self.lsp_buf.clear();
                            // self.terminated = true;
                            // return Poll::Ready(Some(Err(err)));
                        }
                    }
                }
                None => {}
            }

            if self.terminated {
                // EOF with no complete message buffered.
                if self.mode == Some(StreamMode::Control)
                    || !self.raw_buf.is_empty()
                    || !self.lsp_buf.is_empty()
                {
                    self.raw_buf.clear();
                    self.lsp_buf.clear();
                    return Poll::Ready(Some(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "unexpected eof while reading rad message",
                    ))));
                }
                return Poll::Ready(None);
            }

            let filled = {
                let this = &mut *self;
                let mut read_buf = ReadBuf::new(&mut this.read_buf);
                match Pin::new(&mut this.reader).poll_read(cx, &mut read_buf) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => read_buf.filled().len(),
                    Poll::Ready(Err(err)) => {
                        this.terminated = true;
                        return Poll::Ready(Some(Err(err)));
                    }
                }
            };

            if filled == 0 {
                self.terminated = true;
                continue;
            }

            let chunk = self.read_buf[..filled].to_vec();
            self.raw_buf.extend_from_slice(&chunk);
        }
    }
}
