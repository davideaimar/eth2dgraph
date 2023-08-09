use super::trace::Traces;
use super::SerializeDgraph;
use dgraph_tonic::{IClient, Mutate};
use ethabi::{ethereum_types::U256, Address};
use ethers::types::Trace;
use ethers::types::TxHash;
use serde::Deserialize;
use serde::{ser::SerializeStruct, Serialize, Serializer};
use serde_json::json;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractDestruction {
    contract_address: Address,
    tx_hash: TxHash,
    failed: bool,
    block_number: u64,
    balance_left: U256,
    refounded_address: Address,
}

impl From<&Traces> for Vec<ContractDestruction> {
    fn from(traces: &Traces) -> Self {
        let mut destructions = Vec::new();
        for trace in &traces.0 {
            if let Ok(destruction) = ContractDestruction::try_from(trace) {
                destructions.push(destruction);
            }
        }
        destructions
    }
}

impl TryFrom<&Trace> for ContractDestruction {
    type Error = ();

    fn try_from(trace: &Trace) -> Result<Self, Self::Error> {
        let (contract_address, balance_left, refounded_address) = match &trace.action {
            ethers::types::Action::Call(_) => return Err(()),
            ethers::types::Action::Create(_) => return Err(()),
            ethers::types::Action::Suicide(s) => (
                s.address.clone(),
                s.balance.clone(),
                s.refund_address.clone(),
            ),
            ethers::types::Action::Reward(_) => return Err(()),
        };
        let failed = trace.error.is_some();
        let block_number = trace.block_number;
        let tx_hash = trace.transaction_hash.as_ref().unwrap().clone();
        Ok(Self {
            tx_hash,
            failed,
            contract_address,
            block_number,
            balance_left,
            refounded_address,
        })
    }
}

impl ContractDestruction {
    pub fn contract_address(&self) -> Address {
        self.contract_address
    }

    pub async fn upsert<S: IClient>(
        &self,
        dgraph_client: &dgraph_tonic::ClientVariant<S>,
    ) -> Result<(), anyhow::Error> {
        let contract_address = format!("{:?}", self.contract_address);
        let balance_left = &self.balance_left;
        let tx_hash = format!("{:?}", self.tx_hash);
        let failed = self.failed;
        let refound_address = format!("{:?}", self.refounded_address);
        let block_number = self.block_number;

        let query = format!(
            r#"
            query {{
                var(func: eq(Block.number, {block_number})) {{
                    Block as uid
                }}
                var(func: eq(Account.address, "{contract_address}")) {{
                    Contract as uid
                }}
                var(func: eq(Account.address, "{refound_address}")) {{
                    Refound as uid
                }}
            }}
        "#,
            block_number = block_number,
            contract_address = contract_address,
            refound_address = refound_address
        );

        let set = format!(
            r#"
            uid(Block) <Block.number> "{block_number}" .
            uid(Block) <dgraph.type> "Block" .
            uid(Contract) <Account.address> "{contract_address}" .
            uid(Contract) <Account.is_contract> "true" .
            uid(Contract) <dgraph.type> "Account" .
            uid(Refound) <Account.address> "{refound_address}" .
            uid(Refound) <dgraph.type> "Account" .
            _:destr <dgraph.type> "ContractDestruction" .
            _:destr <ContractDestruction.contract> uid(Contract) .
            _:destr <ContractDestruction.balance_left> "{balance_left}" .
            _:destr <ContractDestruction.tx_hash> "{tx_hash}" .
            _:destr <ContractDestruction.failed> "{failed}" .
            _:destr <ContractDestruction.refound_address> uid(Refound) .
            _:destr <ContractDestruction.block> uid(Block) .
        "#,
            block_number = block_number,
            tx_hash = tx_hash,
            balance_left = balance_left,
            failed = failed,
            contract_address = contract_address,
            refound_address = refound_address
        );

        let mut mu = dgraph_tonic::Mutation::new();
        mu.set_set_nquads(set);
        let mut txn = dgraph_client.new_mutated_txn();
        txn.upsert(query, mu).await?;
        txn.commit().await
    }

    fn serialize_dgraph<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Uid {
            uid: String,
        }
        let mut state = serializer.serialize_struct("ContractDestruction", 7)?;
        state.serialize_field("dgraph.type", &json!(["ContractDestruction"]))?;
        state.serialize_field(
            "ContractDestruction.contract",
            &json!({
                "uid": format!("_:{:?}", self.contract_address),
                "dgraph.type": "Account",
                "Account.address": format!("{:?}", self.contract_address),
                "Account.is_contract": true,
            }),
        )?;
        state.serialize_field("ContractDestruction.balance_left", &self.balance_left)?;
        state.serialize_field("ContractDestruction.tx_hash", &self.tx_hash)?;
        state.serialize_field("ContractDestruction.failed", &self.failed)?;
        state.serialize_field(
            "ContractDestruction.refound_address",
            &json!({
                "uid": format!("_:{:?}", self.refounded_address),
                "dgraph.type": ["Account"],
                "Account.address": format!("{:?}", self.refounded_address)
            }),
        )?;
        state.serialize_field(
            "ContractDestruction.block",
            &Uid {
                uid: format!("_:{}", self.block_number),
            },
        )?;
        state.end()
    }
}

impl SerializeDgraph for ContractDestruction {
    fn serialize_dgraph<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.serialize_dgraph(serializer)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        extraction::traces::get_traces, models::contract_destruction::ContractDestruction,
    };
    use ethers::providers::Provider;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_destruction_serialization() {
        let eth_node = std::env::var("ETH_NODE").expect("ETH_NODE env var is not set");

        let eth_client = Arc::new(Provider::try_from(eth_node).unwrap());

        let block = 5426322;

        let creation_traces = get_traces(block, eth_client).await.unwrap();
        let destructions: Vec<ContractDestruction> = Vec::from(&creation_traces);

        for destruction in destructions {
            let mut serializer = serde_json::Serializer::new(Vec::new());
            destruction.serialize_dgraph(&mut serializer).unwrap();
            println!("{}", String::from_utf8(serializer.into_inner()).unwrap());
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_destruction_upsert() {
        let dgraph_endpoint = std::env::var("DGRAPH").expect("Dgraph endpoint");
        let eth_endpoint = std::env::var("ETH_NODE").expect("Ethereum endpoint");
        println!("Connecting to dgraph at {}", dgraph_endpoint);
        println!("Connecting to eth at {}", eth_endpoint);

        let eth_client = Arc::new(Provider::try_from(eth_endpoint).unwrap());
        let dgraph = dgraph_tonic::Client::new(dgraph_endpoint.clone()).expect("Dgraph client");

        let block = 16100062u64;

        let traces = get_traces(block, eth_client).await.unwrap();
        let destructions: Vec<ContractDestruction> = Vec::from(&traces);

        let destr_to_test = destructions.get(0).unwrap();
        destr_to_test.upsert(&dgraph).await.unwrap();
    }
}
