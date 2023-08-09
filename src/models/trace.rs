use ethabi::Address;
use ethers::types::TxHash;
use ethers::types::{Bytes, Trace};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub struct Traces(pub Vec<Trace>);

pub struct CreationTrace {
    failed: bool,
    address: Address,
    creator: Address,
    tx_hash: TxHash,
    deployed_code: Bytes,
    creation_code: Bytes,
}

impl TryFrom<&Trace> for CreationTrace {
    type Error = ();

    fn try_from(trace: &Trace) -> Result<Self, Self::Error> {
        if trace.result.is_none() {
            return Err(());
        }
        let tx_hash = trace.transaction_hash.unwrap();
        let res = trace.result.as_ref().unwrap();
        let (address, deployed_code) = match res {
            ethers::types::Res::Call(_) => return Err(()),
            ethers::types::Res::Create(r) => (r.address, r.code.clone()),
            ethers::types::Res::None => return Err(()),
        };
        let (creation_code, creator) = match &trace.action {
            ethers::types::Action::Call(_) => return Err(()),
            ethers::types::Action::Create(a) => (a.init.clone(), a.from),
            ethers::types::Action::Suicide(_) => return Err(()),
            ethers::types::Action::Reward(_) => return Err(()),
        };
        Ok(CreationTrace {
            failed: false,
            address,
            creator,
            deployed_code,
            creation_code,
            tx_hash,
        })
    }
}

impl CreationTrace {
    pub fn new(
        failed: bool,
        address: Address,
        creator: Address,
        deployed_code: Bytes,
        creation_code: Bytes,
        tx_hash: TxHash,
    ) -> Self {
        Self {
            failed,
            address,
            creator,
            deployed_code,
            creation_code,
            tx_hash,
        }
    }

    pub fn tx_hash(&self) -> TxHash {
        self.tx_hash
    }

    pub fn set_failed(&mut self, failed: bool) {
        self.failed = failed;
    }

    pub fn failed(&self) -> bool {
        self.failed
    }

    pub fn address(&self) -> Address {
        self.address
    }

    pub fn creator(&self) -> Address {
        self.creator
    }

    pub fn deployed_code(&self) -> &Bytes {
        &self.deployed_code
    }

    pub fn creation_code(&self) -> &Bytes {
        &self.creation_code
    }
}

impl From<Vec<Trace>> for Traces {
    fn from(traces: Vec<Trace>) -> Self {
        Self(traces)
    }
}

impl Traces {
    /// Returns a vector of tuples containing the creation traces and a boolean indicating if the
    /// transaction that created the contract failed or not.
    pub fn get_creation_traces(&self) -> Vec<CreationTrace> {
        let failed_tx: Vec<TxHash> = self
            .0
            .iter()
            .filter(|t| t.error.is_some() && t.transaction_hash.is_some())
            .map(|t| t.transaction_hash.unwrap())
            .collect();

        self.0
            .iter()
            .filter_map(|t| match CreationTrace::try_from(t) {
                Ok(mut ct) => {
                    let failed = failed_tx.contains(&ct.tx_hash());
                    ct.set_failed(failed);
                    Some(ct)
                }
                Err(_) => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::extraction::traces::get_traces;
    use ethers::providers::Provider;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_creation_trace_multiple_blocks() {
        let eth_node = std::env::var("ETH_NODE").expect("ETH_NODE env var is not set");

        let eth_client = Arc::new(Provider::try_from(eth_node).unwrap());

        let mut from = 14752490;
        let to = 14752500;

        while from < to {
            let creation_traces = get_traces(from, eth_client.clone()).await.unwrap();
            let creation_traces = creation_traces.get_creation_traces();
            println!(
                "Block: {}, Creation traces: {}",
                from,
                creation_traces.len()
            );
            let failes_ones = creation_traces
                .iter()
                .filter(|t| t.failed() == true)
                .count();
            println!("Failed creations: {}", failes_ones);
            from += 1;
        }
    }

    #[tokio::test]
    async fn test_failed_creation_trace() {
        let eth_node = std::env::var("ETH_NODE").expect("ETH_NODE env var is not set");

        let eth_client = Arc::new(Provider::try_from(eth_node).unwrap());

        let block = 14752490;

        let creation_traces = get_traces(block, eth_client).await.unwrap();
        let creation_traces = creation_traces.get_creation_traces();

        assert_eq!(creation_traces.len(), 2);

        let failed_trace = creation_traces.iter().find(|t| t.failed() == true).unwrap();

        assert_eq!(
            format!("{:?}", failed_trace.tx_hash()),
            "0x77cbc1ea534b5bfb67e165c5ed3fae903f1a51445c7b18136de596031243e445".to_string()
        );

        assert_eq!(
            format!("{:?}", failed_trace.address()),
            "0x71fd0a70c22198ada589dd6c1cb5b6df937aa81d".to_string()
        );
    }
}
