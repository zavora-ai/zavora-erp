use async_trait::async_trait;
use uuid::Uuid;

#[async_trait]
pub trait MessagingTool: Send + Sync {
    async fn send_message(&self, recipient: &str, subject: &str, body: &str) -> anyhow::Result<()>;
}

#[async_trait]
pub trait InventoryTool: Send + Sync {
    async fn quantity_available(&self, item_code: &str) -> anyhow::Result<f64>;
}

#[async_trait]
pub trait CommitmentTool: Send + Sync {
    async fn create_sales_commitment(&self, customer: &str, quote: &str) -> anyhow::Result<Uuid>;
}
