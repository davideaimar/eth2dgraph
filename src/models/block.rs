use super::SerializeDgraph;
use anyhow::{bail, Ok};
use chrono::{NaiveDateTime, TimeZone, Utc};
use dgraph_tonic::{IClient, Mutate};
use ethabi::ethereum_types::U256;
use serde::{ser::SerializeStruct, Serializer};
use serde_json::json;
use std::ops::{Deref, DerefMut};

#[derive(Debug)]
pub struct Block(ethers::types::Block<ethers::types::Transaction>);

impl Block {
    pub fn get_number(&self) -> u64 {
        self.number.unwrap().as_u64()
    }

    pub fn get_difficulty(&self) -> U256 {
        self.difficulty
    }

    pub fn get_timestamp(&self) -> u64 {
        self.0.timestamp.as_u64()
    }

    pub fn get_rfc3339(&self) -> String {
        let timestamp = self.get_timestamp() * 1000;
        let datetime = NaiveDateTime::from_timestamp_millis(timestamp as i64).unwrap();
        Utc.from_utc_datetime(&datetime).to_rfc3339()
    }

    /// get info about gas price in Gwei
    /// returns (min, max, avg, std_dev)
    pub fn get_gas_price_data(&self) -> (f64, f64, f64, f64) {
        let prices = &self
            .0
            .transactions
            .iter()
            .filter(|tx| tx.gas_price.is_some())
            .map(|tx| tx.gas_price.unwrap().as_u128() as f64 / 1e9)
            .collect::<Vec<f64>>();

        let (max, min, sum, cnt): (f64, f64, f64, usize) = prices.iter().fold(
            (0.0, std::f64::MAX, 0.0, 0),
            |(max, min, sum, cnt), gas_price| {
                (
                    gas_price.max(max),
                    gas_price.min(min),
                    sum + gas_price,
                    cnt + 1,
                )
            },
        );

        let avg = sum / cnt as f64;

        let std_dev = prices.iter().fold(0.0, |std_dev, gas_price| {
            std_dev + (gas_price - avg).powi(2)
        }) / cnt as f64;

        let std_dev = std_dev.sqrt();

        (min, max, avg, std_dev)
    }

    pub async fn upsert<S: IClient>(
        &self,
        dgraph_client: &dgraph_tonic::ClientVariant<S>,
    ) -> Result<(), anyhow::Error> {
        // fields

        let block_no = self.get_number();
        let diffifulty = self.get_difficulty();
        let datetime = self.get_rfc3339();
        let tx_count = self.0.transactions.len() as u64;
        let (min, max, avg, std_dev) = self.get_gas_price_data();
        let gas_limit = self.0.gas_limit.as_u64();
        let gas_used = self.0.gas_used.as_u64();

        let base_fee_per_gas = if let Some(base_fee_per_gas) = &self.base_fee_per_gas {
            Some(base_fee_per_gas.as_u128() as f64 / 1e9)
        } else {
            None
        };
        let size = if let Some(size) = &self.size {
            Some(size.as_u64())
        } else {
            None
        };

        if self.author.is_none() {
            bail!("Block {} has no author", block_no);
        }

        let miner_address = format!("{:?}", self.author.as_ref().unwrap());

        // Query part of the upsert
        let query = format!(
            r#"
            query {{
              var(func: eq(Block.number, {block_no})) {{
                Block as uid
              }}
              var(func: eq(Account.number, {miner_address})) {{
                Miner as uid
              }}
            }}
        "#,
            block_no = block_no,
            miner_address = miner_address,
        );

        // Mutation part of the upsert
        let mut set = format!(
            r#"
            uid(Miner) <dgraph.type> "Account" .
            uid(Miner) <Account.address> "{miner_address}" .

            uid(Block) <dgraph.type> "Block" .
            uid(Block) <Block.number> "{block_no}" .
            uid(Block) <Block.difficulty> "{difficulty}" .
            uid(Block) <Block.datetime> "{datetime}" .
            uid(Block) <Block.tx_count> "{tx_count}" .
            uid(Block) <Block.gas_price_min> "{min}" .
            uid(Block) <Block.gas_price_max> "{max}" .
            uid(Block) <Block.gas_price_avg> "{avg}" .
            uid(Block) <Block.gas_price_std_dev> "{std_dev}" .
            uid(Block) <Block.gas_limit> "{gas_limit}" .
            uid(Block) <Block.gas_used> "{gas_used}" .
        "#,
            block_no = block_no,
            difficulty = diffifulty,
            datetime = datetime,
            tx_count = tx_count,
            min = min,
            max = max,
            avg = avg,
            std_dev = std_dev,
            gas_limit = gas_limit,
            gas_used = gas_used,
            miner_address = miner_address,
        );

        if base_fee_per_gas.is_some() {
            set.push_str(&format!(
                r#"uid(Block) <Block.base_fee_per_gas> "{base_fee_per_gas}" .
                "#,
                base_fee_per_gas = base_fee_per_gas.unwrap(),
            ));
        }
        if size.is_some() {
            set.push_str(&format!(
                r#"uid(Block) <Block.size> "{size}" .
                "#,
                size = size.unwrap(),
            ));
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

    /// Delete all logs related to this block in Dgraph
    pub async fn upsert_delete_logs<S: IClient>(
        block_no: u64,
        dgraph_client: &dgraph_tonic::ClientVariant<S>,
    ) -> Result<(), anyhow::Error> {
        let query = format!(
            r#"
            query {{
                var(func: eq(Block.number, {block_no})) {{
                    ~Log.block {{
                        log as uid
                    }}
                }}
            }}
            "#,
            block_no = block_no
        );

        let delete = r#"
            uid(log) * * .
        "#;

        let mut mu = dgraph_tonic::Mutation::new();
        mu.set_delete_nquads(delete);
        let mut txn = dgraph_client.new_mutated_txn();
        txn.upsert(query, mu).await?;
        txn.commit().await
    }

    /// Delete all contract destructions related to this block in Dgraph
    pub async fn upsert_delete_destructions<S: IClient>(
        block_no: u64,
        dgraph_client: &dgraph_tonic::ClientVariant<S>,
    ) -> Result<(), anyhow::Error> {
        let query = format!(
            r#"
            query {{
                var(func: eq(Block.number, {block_no})) {{
                    ~ContractDestruction.block {{
                        destr as uid
                    }}
                }}
            }}
            "#,
            block_no = block_no
        );

        let delete = r#"
            uid(destr) * * .
        "#;

        let mut mu = dgraph_tonic::Mutation::new();
        mu.set_delete_nquads(delete);
        let mut txn = dgraph_client.new_mutated_txn();
        txn.upsert(query, mu).await?;
        txn.commit().await
    }

    /// Delete all contract deployments related to this block in Dgraph
    pub async fn upsert_delete_deployments<S: IClient>(
        block_no: u64,
        dgraph_client: &dgraph_tonic::ClientVariant<S>,
    ) -> Result<(), anyhow::Error> {
        let query = format!(
            r#"
            query {{
                var(func: eq(Block.number, {block_no})) {{
                    ~ContractDeployment.block {{
                        deploy as uid
                    }}
                }}
            }}
            "#,
            block_no = block_no
        );

        let delete = r#"
            uid(deploy) * * .
        "#;

        let mut mu = dgraph_tonic::Mutation::new();
        mu.set_delete_nquads(delete);
        let mut txn = dgraph_client.new_mutated_txn();
        txn.upsert(query, mu).await?;
        txn.commit().await
    }

    pub async fn upsert_delete_transfers<S: IClient>(
        block_no: u64,
        dgraph_client: &dgraph_tonic::ClientVariant<S>,
    ) -> Result<(), anyhow::Error> {
        let query = format!(
            r#"
            query {{
                var(func: eq(Block.number, {block_no})) {{
                    ~TokenTransfer.block {{
                        transfer as uid
                    }}
                }}
            }}
            "#,
            block_no = block_no
        );

        let delete = r#"
            uid(transfer) * * .
        "#;

        let mut mu = dgraph_tonic::Mutation::new();
        mu.set_delete_nquads(delete);
        let mut txn = dgraph_client.new_mutated_txn();
        txn.upsert(query, mu).await?;
        txn.commit().await
    }

    fn serialize_dgraph<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Block", 10)?;
        state.serialize_field("uid", format!("_:{}", self.get_number()).as_str())?;
        state.serialize_field("dgraph.type", "Block")?;
        state.serialize_field("Block.number", &self.get_number())?;
        state.serialize_field("Block.difficulty", &self.get_difficulty().to_string())?;
        state.serialize_field("Block.datetime", &self.get_rfc3339())?;
        state.serialize_field("Block.tx_count", &self.0.transactions.len())?;
        let (min, max, avg, std_dev) = self.get_gas_price_data();
        state.serialize_field("Block.gas_price_min", &min)?;
        state.serialize_field("Block.gas_price_max", &max)?;
        state.serialize_field("Block.gas_price_avg", &avg)?;
        state.serialize_field("Block.gas_price_std_dev", &std_dev)?;
        state.serialize_field("Block.gas_limit", &self.gas_limit.as_u64())?;
        state.serialize_field("Block.gas_used", &self.gas_used.as_u64())?;
        if let Some(author) = &self.author {
            state.serialize_field(
                "Block.miner",
                &json!({
                    "uid": &format!("_:{:?}", author),
                    "dgraph.type": "Account",
                    "Account.address": &format!("{:?}", author),
                }),
            )?;
        }
        if let Some(base_fee_per_gas) = &self.base_fee_per_gas {
            state.serialize_field(
                "Block.base_fee_per_gas",
                &(base_fee_per_gas.as_u128() as f64 / 1e9),
            )?;
        }
        if let Some(size) = &self.size {
            state.serialize_field("Block.size", &size.as_u64())?;
        }
        if let Some(withdrawals) = &self.withdrawals {
            let mut serialized_withdrawals = Vec::with_capacity(withdrawals.len());
            for withdrawal in withdrawals {
                serialized_withdrawals.push(json!({
                    "dgraph.type": "Withdrawal",
                    "Withdrawal.address": {
                        "uid": &format!("_:{:?}", withdrawal.address),
                        "dgraph.type": "Account",
                        "Account.address": &format!("{:?}", withdrawal.address),
                    },
                    "Withdrawal.amount": &withdrawal.amount.to_string(),
                    "Withdrawal.index": withdrawal.index.as_u64(),
                    "Withdrawal.validator_index": withdrawal.validator_index.as_u64(),
                }));
            }
            state.serialize_field("Block.withdrawals", &serialized_withdrawals)?;
        }
        state.end()
    }
}

impl SerializeDgraph for Block {
    fn serialize_dgraph<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.serialize_dgraph(serializer)
    }
}

impl Deref for Block {
    type Target = ethers::types::Block<ethers::types::Transaction>;
    fn deref(&self) -> &ethers::types::Block<ethers::types::Transaction> {
        &self.0
    }
}

impl DerefMut for Block {
    fn deref_mut(&mut self) -> &mut ethers::types::Block<ethers::types::Transaction> {
        &mut self.0
    }
}

impl From<ethers::types::Block<ethers::types::Transaction>> for Block {
    fn from(block: ethers::types::Block<ethers::types::Transaction>) -> Self {
        Block(block)
    }
}

#[cfg(test)]
mod tests {
    use ethers::providers::Provider;
    use std::sync::Arc;

    use crate::extraction::blocks::get_block;

    #[tokio::test]
    async fn block_serialization() {
        let eth_node = std::env::var("ETH_NODE").expect("ETH_NODE env var is not set");

        let eth_client = Arc::new(Provider::try_from(eth_node).unwrap());

        let block = get_block(17200004, eth_client).await.unwrap().unwrap();

        let price_data = block.get_gas_price_data();

        println!("{:?}", price_data);

        let mut serializer = serde_json::Serializer::new(Vec::new());
        block.serialize_dgraph(&mut serializer).unwrap();
        println!("{}", String::from_utf8(serializer.into_inner()).unwrap());
    }

    #[tokio::test]
    #[ignore]
    async fn test_block_upsert() {
        let dgraph_endpoint = std::env::var("DGRAPH").expect("Dgraph endpoint");
        let eth_endpoint = std::env::var("ETH_NODE").expect("Ethereum endpoint");
        println!("Connecting to dgraph at {}", dgraph_endpoint);
        println!("Connecting to eth at {}", eth_endpoint);

        let eth_client = Arc::new(Provider::try_from(eth_endpoint).unwrap());
        let dgraph = dgraph_tonic::Client::new(dgraph_endpoint.clone()).expect("Dgraph client");

        let block = 16100001u64;

        let block = get_block(block, eth_client).await.unwrap().unwrap();

        block.upsert(&dgraph).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_block_delete_logs() {
        let dgraph_endpoint = std::env::var("DGRAPH").expect("Dgraph endpoint");
        println!("Connecting to dgraph at {}", dgraph_endpoint);

        let dgraph = dgraph_tonic::Client::new(dgraph_endpoint.clone()).expect("Dgraph client");

        let block = 16100001u64;

        crate::models::block::Block::upsert_delete_logs(block, &dgraph)
            .await
            .unwrap();
    }
}
