use ethers::providers::Middleware;
use std::sync::Arc;

use crate::models::block::Block;

pub async fn get_block<T>(
    block: u64,
    eth_client: Arc<T>,
) -> Result<Option<Block>, <T as Middleware>::Error>
where
    T: Middleware,
{
    if let Some(block) = eth_client.get_block_with_txs(block).await? {
        Ok(Some(block.into()))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethers::providers::Provider;

    #[tokio::test]
    async fn test_get_block() {
        let eth_node = std::env::var("ETH_NODE").expect("ETH_NODE env var is not set");

        let eth_client = Arc::new(Provider::try_from(eth_node).unwrap());

        let block = 1000000;

        let b = get_block(block, eth_client).await.unwrap();

        assert_eq!(b.unwrap().get_number(), block);
    }
}
