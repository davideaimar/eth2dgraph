use crate::models::trace::Traces;
use ethers::types::TxHash;
use ethers::{providers::Middleware, types::Trace};
use std::{collections::HashMap, sync::Arc};

fn propagate_errors(traces: &mut Vec<Trace>) {
    // group traces by transaction hash
    let mut txs: HashMap<TxHash, Vec<&mut Trace>> = HashMap::new();
    traces.iter_mut().for_each(|t| {
        if t.transaction_hash.is_some() {
            let group = txs
                .entry(t.transaction_hash.unwrap().clone())
                .or_insert(vec![]);
            group.push(t);
        }
    });
    // inside each transaction, mark trace as failed if a parent trace has failed
    txs.iter_mut().for_each(|(_, grouped_traces)| {
        // collect trace addresses of failed traces
        let failed = grouped_traces
            .iter()
            .filter(|t| t.error.is_some())
            .map(|t| t.trace_address.clone())
            .collect::<Vec<Vec<usize>>>();
        // loop again traces to flag ones whose parent failed
        grouped_traces.iter_mut().for_each(|t| {
            let address = t.trace_address.as_slice();
            let parent_failed = failed.iter().any(|f| address.starts_with(f));
            if parent_failed {
                t.error = Some("Parent failed".to_string());
            }
        });
    });
}

pub async fn get_traces<T>(
    block: u64,
    eth_client: Arc<T>,
) -> Result<Traces, <T as Middleware>::Error>
where
    T: Middleware,
{
    let traces = eth_client.trace_block(block.into()).await;
    if traces.is_err() {
        return Err(traces.err().unwrap());
    }
    let mut traces = traces.unwrap();
    propagate_errors(&mut traces); // ensure all failed traces are marked as such
    Ok(traces.into())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use ethers::providers::Provider;

    #[tokio::test]
    async fn test_get_traces() {
        let eth_node = std::env::var("ETH_NODE").expect("ETH_NODE env var is not set");

        let eth_client = Arc::new(Provider::try_from(eth_node).unwrap());

        let block = 1;

        get_traces(block, eth_client).await.unwrap();
    }

    #[tokio::test]
    async fn test_huge_trace_block() {
        let eth_node = std::env::var("ETH_NODE").expect("ETH_NODE env var is not set");

        let eth_client = Arc::new(Provider::try_from(eth_node).unwrap());

        let block = 14174380;

        get_traces(block, eth_client).await.unwrap();
    }

    #[tokio::test]
    async fn test_creation_traces() {
        let eth_node = std::env::var("ETH_NODE").expect("ETH_NODE env var is not set");

        let eth_client = Arc::new(Provider::try_from(eth_node).unwrap());

        let block = 4719568;

        let traces = get_traces(block, eth_client).await.unwrap();
        println!("{:?}", traces);
        let contracts = traces.get_creation_traces();

        assert_eq!(contracts.len(), 2);
    }

    #[tokio::test]
    async fn test_propagate_errors() {
        let eth_node = std::env::var("ETH_NODE").expect("ETH_NODE env var is not set");

        let eth_client = Arc::new(Provider::try_from(eth_node).unwrap());

        let block = 16_634_562;

        let traces = get_traces(block, eth_client).await.unwrap();

        traces
            .0
            .iter()
            .filter(|t| {
                t.transaction_hash.is_some()
                    && t.transaction_hash.unwrap().eq(&TxHash::from_str(
                        "0x32572f8933466b75c387ef64a36cffc72a9c467e5680be031d3f419509920041",
                    )
                    .unwrap())
            })
            .for_each(|t| {
                if t.trace_address.len() >= 2
                    && t.trace_address.get(0).unwrap().to_owned() == 3
                    && t.trace_address.get(1).unwrap().to_owned() == 0
                {
                    assert!(t.error.is_some());
                } else {
                    assert!(t.error.is_none());
                }
            });
    }
}
