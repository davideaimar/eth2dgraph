use ethabi::{ParamType, Token};
use ethers::types::{Address, H256, U256};
use ethers::{
    providers::Middleware,
    types::{Filter, Log, Topic},
    utils::keccak256,
};
use std::sync::Arc;

use crate::models::transfer::{TokenTransfer, TokenType};

pub async fn get_transfer_logs<T>(
    block: u64,
    eth_client: Arc<T>,
) -> Result<Vec<Log>, <T as Middleware>::Error>
where
    T: Middleware,
{
    let transfer_event_sig = keccak256(b"Transfer(address,address,uint256)");

    // filter only for Transfer events
    let filter = Filter::new()
        .from_block(block)
        .to_block(block)
        .topic0(Topic::Value(Some(transfer_event_sig.into())));

    let logs: Vec<Log> = eth_client.get_logs(&filter).await?;

    Ok(logs)
}

pub async fn get_all_logs<T>(
    block: u64,
    eth_client: Arc<T>,
) -> Result<Vec<Log>, <T as Middleware>::Error>
where
    T: Middleware,
{
    // Get all logs at block <block>
    let filter = Filter::new().from_block(block).to_block(block);

    eth_client.get_logs(&filter).await
}

pub fn get_transfer_from_logs(logs: &[Log]) -> Vec<TokenTransfer> {
    let transfer_event_sig = keccak256(b"Transfer(address,address,uint256)");

    let mut transfers = Vec::new();

    for log in logs {
        if !log.topics.is_empty() && log.topics[0] == transfer_event_sig.into() {
            let token_type = if log.topics.len() == 3 {
                TokenType::ERC20
            } else if log.topics.len() == 4 {
                TokenType::ERC721
            } else {
                continue;
            };

            let data: Vec<H256> = log
                .data
                .chunks_exact(256 / 8)
                .map(H256::from_slice)
                .collect();
            // merge the data and topics

            let mut params_data = log.topics.clone();
            params_data.extend(data);
            // param 1 is the from address
            // param 2 is the to address
            // param 3 is the value
            if params_data.len() != 4 {
                continue;
            }
            let params_types =
                [ParamType::Address, ParamType::Address, ParamType::Uint(256)].as_slice();

            let params_data = params_data
                .into_iter()
                .flat_map(|x| x.as_bytes().to_vec())
                .collect::<Vec<u8>>();

            let params = ethabi::decode_whole(params_types, &params_data[32..]);

            if let Ok(params) = params {
                let from: Address = match params[0] {
                    Token::Address(ref addr) => *addr,
                    _ => continue,
                };
                let to: Address = match params[1] {
                    Token::Address(ref addr) => *addr,
                    _ => continue,
                };
                let value: U256 = match params[2] {
                    Token::Uint(ref value) => *value,
                    _ => continue,
                };

                transfers.push(TokenTransfer::new(
                    log.address,
                    from,
                    to,
                    value,
                    log.block_number.unwrap(),
                    log.transaction_hash.unwrap().0.into(),
                    token_type,
                ));
            }
        }
    }

    transfers
}

#[cfg(test)]
mod tests {

    use super::*;
    use ethers::providers::Provider;

    #[tokio::test]
    async fn test_get_logs() {
        let eth_node = std::env::var("ETH_NODE").expect("ETH_NODE env var is not set");

        let eth_client = Arc::new(Provider::try_from(eth_node).unwrap());

        let block = 1000000;

        get_transfer_logs(block, eth_client).await.unwrap();
    }

    #[tokio::test]
    async fn test_analyze_logs() {
        let eth_node = std::env::var("ETH_NODE").expect("ETH_NODE env var is not set");

        let eth_client = Arc::new(Provider::try_from(eth_node).unwrap());

        let block = 10000000;

        let logs = get_transfer_logs(block, eth_client).await.unwrap();

        let transfers = get_transfer_from_logs(&logs);

        println!("{:?}", transfers);
    }
}
