use anyhow::Result;
use async_trait::async_trait;
use zavora_tools::{CommitmentTool, InventoryTool, MessagingTool};

#[async_trait]
pub trait AgentLoop {
    async fn tick(&self) -> Result<()>;
}

pub struct SalesAgent<TMessage, TInventory, TCommitment>
where
    TMessage: MessagingTool,
    TInventory: InventoryTool,
    TCommitment: CommitmentTool,
{
    pub messaging: TMessage,
    pub inventory: TInventory,
    pub commitments: TCommitment,
}

#[async_trait]
impl<TMessage, TInventory, TCommitment> AgentLoop for SalesAgent<TMessage, TInventory, TCommitment>
where
    TMessage: MessagingTool + Send + Sync,
    TInventory: InventoryTool + Send + Sync,
    TCommitment: CommitmentTool + Send + Sync,
{
    async fn tick(&self) -> Result<()> {
        let _ = self.inventory.quantity_available("SKU-001").await?;
        Ok(())
    }
}

pub struct BoardAgent;

#[async_trait]
impl AgentLoop for BoardAgent {
    async fn tick(&self) -> Result<()> {
        Ok(())
    }
}
