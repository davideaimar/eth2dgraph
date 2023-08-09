use super::SerializeDgraph;
use dgraph_tonic::{IClient, Mutate};
use serde::{ser::SerializeStruct, Serializer};
use serde_json::json;
use std::ops::{Deref, DerefMut};

#[derive(Debug)]
pub struct Log(ethers::types::Log);

impl From<ethers::types::Log> for Log {
    fn from(tx: ethers::types::Log) -> Self {
        Self(tx)
    }
}

impl Deref for Log {
    type Target = ethers::types::Log;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Log {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Log {
    pub async fn upsert<S: IClient>(
        &self,
        dgraph_client: &dgraph_tonic::ClientVariant<S>,
    ) -> Result<(), anyhow::Error> {
        // WARNING:
        // Logs don't have a unique identifier
        // upserting already existing logs will result in a duplicate
        // This function should be called just after checking if the log
        // of a certain block already exists, or after deleting them using Block::upsert_delete_logs

        let block_no = self.block_number.as_ref().unwrap().as_u64();
        let contract_address = format!("{:?}", self.address);
        let tx_hash = format!("{:?}", self.transaction_hash.as_ref().unwrap());
        let data = self.data.to_string();
        let tx_index = self.transaction_index.as_ref().unwrap().as_u64();
        let index = self.log_index.as_ref().unwrap().as_u64();

        // Query part of the upsert
        let query = format!(
            r#"
            query {{
              var(func: eq(Block.number, {block_no})) {{
                Block as uid
              }}
              var(func: eq(Transaction.hash, "{tx_hash}")) {{
                Tx as uid
              }}
              var(func: eq(Account.address, "{contract_address}")) {{
                Contract as uid
              }}
            }}
        "#,
            block_no = block_no,
            tx_hash = tx_hash,
            contract_address = contract_address
        );

        // Mutation part of the upsert
        let mut set = format!(
            r#"
            uid(Block) <Block.number> "{block_no}" .
            uid(Block) <dgraph.type> "Block" .
            uid(Tx) <Transaction.hash> "{tx_hash}" .
            uid(Tx) <dgraph.type> "Transaction" .
            uid(Contract) <Account.address> "{contract_address}" .
            uid(Contract) <Account.is_contract> "true" .
            uid(Contract) <dgraph.type> "Account" .
            _:log <dgraph.type> "Log" .
            _:log <Log.block> uid(Block) .
            _:log <Log.transaction> uid(Tx) .
            _:log <Log.contract> uid(Contract) .
            _:log <Log.data> "{data}" .
            _:log <Log.tx_index> "{tx_index}" .
            _:log <Log.index> "{index}" .
        "#,
            block_no = block_no,
            tx_hash = tx_hash,
            contract_address = contract_address,
            data = data,
            tx_index = tx_index,
            index = index
        );

        for (i, topic) in self.topics.iter().enumerate() {
            match i {
                0 => {
                    set.push_str(&format!(
                        r#"_:log <Log.topic_0> "{topic_0}" .
                        "#,
                        topic_0 = format!("{:?}", topic)
                    ));
                }
                1 => {
                    set.push_str(&format!(
                        r#"_:log <Log.topic_1> "{topic_1}" .
                        "#,
                        topic_1 = format!("{:?}", topic)
                    ));
                }
                2 => {
                    set.push_str(&format!(
                        r#"_:log <Log.topic_2> "{topic_2}" .
                        "#,
                        topic_2 = format!("{:?}", topic)
                    ));
                }
                3 => {
                    set.push_str(&format!(
                        r#"_:log <Log.topic_3> "{topic_3}" .
                        "#,
                        topic_3 = format!("{:?}", topic)
                    ));
                }
                _ => {
                    break; // should never happen
                }
            }
        }

        // Perform the upsert
        let mut mu = dgraph_tonic::Mutation::new();
        mu.set_set_nquads(set);
        let mut txn = dgraph_client.new_mutated_txn();
        txn.upsert(query, mu).await?;
        txn.commit().await?;
        // println!("Upserting query: {}", query);
        // println!("Upserting set: {}", set);

        Ok(())
    }

    fn serialize_dgraph<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Log", 7)?;
        state.serialize_field("dgraph.type", "Log")?;
        state.serialize_field(
            "Log.contract",
            &json!({
                "uid": format!("_:{:?}", self.address),
                "dgraph.type": "Account",
                "Account.address": format!("{:?}", self.address),
                "Account.is_contract": true,
            }),
        )?;
        for (i, topic) in self.topics.iter().enumerate() {
            match i {
                0 => {
                    state.serialize_field("Log.topic_0", &format!("{:?}", topic))?;
                }
                1 => {
                    state.serialize_field("Log.topic_1", &format!("{:?}", topic))?;
                }
                2 => {
                    state.serialize_field("Log.topic_2", &format!("{:?}", topic))?;
                }
                3 => {
                    state.serialize_field("Log.topic_3", &format!("{:?}", topic))?;
                }
                _ => {
                    break; // should never happen
                }
            }
        }
        state.serialize_field("Log.data", &format!("{}", self.data))?;
        if self.block_number.is_some() {
            state.serialize_field(
                "Log.block",
                &json!({ "uid": format!("_:{:?}", self.block_number.as_ref().unwrap().as_u64()) }),
            )?;
        }
        if self.transaction_hash.is_some() {
            state.serialize_field(
                "Log.tx",
                &json!({ "uid": format!("_:{:?}", self.transaction_hash.as_ref().unwrap()) }),
            )?;
        }
        if self.transaction_index.is_some() {
            state.serialize_field(
                "Log.tx_index",
                &format!("{}", self.transaction_index.as_ref().unwrap()),
            )?;
        }
        if self.log_index.is_some() {
            state.serialize_field(
                "Log.index",
                &format!("{}", self.log_index.as_ref().unwrap()),
            )?;
        }
        if self.removed.is_some() && *self.removed.as_ref().unwrap() {
            // removed indicates whether this log was removed from the blockchain due to a chain reorganization.
            state.serialize_field("Log.removed", &true)?;
        }

        state.end()
    }
}

impl SerializeDgraph for Log {
    fn serialize_dgraph<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.serialize_dgraph(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::Log;
    use crate::{extraction::logs::get_all_logs, models::block::Block};
    use ethers::providers::Provider;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_log_serialization() {
        let eth_node = std::env::var("ETH_NODE").expect("ETH_NODE env var is not set");

        let eth_client = Arc::new(Provider::try_from(eth_node).unwrap());

        let block = 16000000u64;

        let logs = get_all_logs(block, eth_client).await.unwrap();

        for log in logs {
            let mut serializer = serde_json::Serializer::new(Vec::new());
            let log = Log::from(log);
            log.serialize_dgraph(&mut serializer).unwrap();
            println!("{}", String::from_utf8(serializer.into_inner()).unwrap());
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_upsert_whole_block() {
        let dgraph_endpoint = std::env::var("DGRAPH").expect("Dgraph endpoint");
        let eth_endpoint = std::env::var("ETH_NODE").expect("Ethereum endpoint");
        println!("Connecting to dgraph at {}", dgraph_endpoint);
        println!("Connecting to eth at {}", eth_endpoint);

        let eth_client = Arc::new(Provider::try_from(eth_endpoint).unwrap());
        let dgraph = dgraph_tonic::Client::new(dgraph_endpoint.clone()).expect("Dgraph client");

        let block_no = 16100001u64;

        let logs = get_all_logs(block_no, eth_client).await.unwrap();

        let now = tokio::time::Instant::now();

        Block::upsert_delete_logs(block_no, &dgraph)
            .await
            .expect("Delete upsert failed");

        for log in logs {
            let log = super::Log::from(log);
            log.upsert(&dgraph).await.expect("Set upsert failed");
        }

        let elapsed = now.elapsed();

        println!("Block tx upsert took {:?}", elapsed);
    }

    #[tokio::test]
    #[ignore]
    async fn test_log_upsert() {
        let dgraph_endpoint = std::env::var("DGRAPH").expect("Dgraph endpoint");
        let eth_endpoint = std::env::var("ETH_NODE").expect("Ethereum endpoint");
        println!("Connecting to dgraph at {}", dgraph_endpoint);
        println!("Connecting to eth at {}", eth_endpoint);

        let eth_client = Arc::new(Provider::try_from(eth_endpoint).unwrap());
        let dgraph = dgraph_tonic::Client::new(dgraph_endpoint.clone()).expect("Dgraph client");

        let block = 16100001u64;

        let logs = get_all_logs(block, eth_client).await.unwrap();

        let log_to_test: crate::models::log::Log = logs[0].clone().into();
        log_to_test.upsert(&dgraph).await.unwrap();
    }
}
