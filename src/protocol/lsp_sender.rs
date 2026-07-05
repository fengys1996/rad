use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::error::SendError;

use super::lsp::LspFrame;
use super::msg::RadMessage;

#[derive(Clone)]
pub struct LspSender {
    tx: Sender<RadMessage>,
}

impl LspSender {
    pub fn new(tx: Sender<RadMessage>) -> Self {
        Self { tx }
    }

    pub async fn send(&self, frame: LspFrame) -> Result<(), SendError<RadMessage>> {
        self.tx.send(RadMessage::lsp(frame)).await
    }
}
