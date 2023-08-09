use super::SerializeDgraph;
use dgraph_tonic::IClient;
use dgraph_tonic::Mutate;
use serde::{ser::SerializeStruct, Serialize, Serializer};
use std::ops::{Deref, DerefMut};

#[derive(Debug)]
pub struct Transaction(ethers::types::Transaction);

impl From<ethers::types::Transaction> for Transaction {
    fn from(tx: ethers::types::Transaction) -> Self {
        Self(tx)
    }
}

impl Deref for Transaction {
    type Target = ethers::types::Transaction;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Transaction {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Transaction {
    pub async fn upsert<S: IClient>(
        &self,
        dgraph_client: &dgraph_tonic::ClientVariant<S>,
    ) -> Result<(), anyhow::Error> {
        // mandatory
        let block_no = self.block_number.as_ref().unwrap().as_u64();
        let from = format!("{:?}", self.from);
        let to = if self.to.is_some() {
            format!("{:?}", self.to.as_ref().unwrap())
        } else {
            let zero_address = ethers::types::Address::zero();
            format!("{:?}", zero_address)
        };
        let tx_hash = format!("{:?}", self.hash);
        let input = self.input.to_string();
        let nonce = self.nonce.as_u64();
        let value = self.value.to_string();
        let gas = self.gas.as_u64();
        let r = self.r.to_string();
        let s = self.s.to_string();
        let v = self.v.as_u64();

        // optional
        let gas_price = if self.gas_price.is_some() {
            Some(self.gas_price.as_ref().unwrap().as_u64())
        } else {
            None
        };
        let bytes4 = if self.input.len() >= 4 {
            Some(input.get(2..10).unwrap())
        } else {
            None
        };
        let max_fee_per_gas = if self.max_fee_per_gas.is_some() {
            Some(self.max_fee_per_gas.as_ref().unwrap().as_u64())
        } else {
            None
        };
        let max_priority_fee_per_gas = if self.max_priority_fee_per_gas.is_some() {
            Some(self.max_priority_fee_per_gas.as_ref().unwrap().as_u64())
        } else {
            None
        };

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
              var(func: eq(Account.address, "{from}")) {{
                From as uid
              }}
              var(func: eq(Account.address, "{to}")) {{
                To as uid
              }}
            }}
        "#,
            block_no = block_no,
            tx_hash = tx_hash,
            from = from,
            to = to
        );

        // Mutation part of the upsert
        let mut set = format!(
            r#"
            uid(Block) <Block.number> "{block_no}" .
            uid(Block) <dgraph.type> "Block" .
            uid(From) <Account.address> "{from}" .
            uid(From) <dgraph.type> "Account" .
            uid(To) <Account.address> "{to}" .
            uid(To) <dgraph.type> "Account" .

            uid(Tx) <dgraph.type> "Transaction" .
            uid(Tx) <Transaction.hash> "{tx_hash}" .
            uid(Tx) <Transaction.block> uid(Block) .
            uid(Tx) <Transaction.from> uid(From) .
            uid(Tx) <Transaction.to> uid(To) .
            uid(Tx) <Transaction.input> "{input}" .
            uid(Tx) <Transaction.nonce> "{nonce}" .
            uid(Tx) <Transaction.value> "{value}" .
            uid(Tx) <Transaction.gas> "{gas}" .
            uid(Tx) <Transaction.r> "{r}" .
            uid(Tx) <Transaction.s> "{s}" .
            uid(Tx) <Transaction.v> "{v}" .
        "#,
            tx_hash = tx_hash,
            nonce = nonce,
            input = input,
            value = value,
            gas = gas,
            r = r,
            s = s,
            v = v,
        );

        if gas_price.is_some() {
            set.push_str(&format!(
                r#"uid(Tx) <Transaction.gas_price> "{gas_price}" .
            "#,
                gas_price = gas_price.unwrap()
            ));
        }

        if bytes4.is_some() {
            set.push_str(&format!(
                r#"uid(Tx) <Transaction.bytes4> "{bytes4}" .
            "#,
                bytes4 = bytes4.unwrap()
            ));
        }

        if max_fee_per_gas.is_some() {
            set.push_str(&format!(
                r#"uid(Tx) <Transaction.max_fee_per_gas> "{max_fee_per_gas}" .
                "#,
                max_fee_per_gas = max_fee_per_gas.unwrap()
            ));
        }

        if max_priority_fee_per_gas.is_some() {
            set.push_str(&format!(
                r#"uid(Tx) <Transaction.max_priority_fee_per_gas> "{max_priority_fee_per_gas}" .
                "#,
                max_priority_fee_per_gas = max_priority_fee_per_gas.unwrap()
            ));
        }

        // Perform the upsert
        let mut mu = dgraph_tonic::Mutation::new();
        mu.set_set_nquads(set);
        let mut txn = dgraph_client.new_mutated_txn();
        txn.upsert(query, mu).await?;
        txn.commit().await?;

        Ok(())
    }

    async fn _bake_tx<M: Mutate>(&self, dgraph_mut_tx: &mut M) -> Result<(), anyhow::Error> {
        // Unused test for seeing if doing everything in one transaction is faster
        // It resulted in being slower, so I'm not using it
        // I leave it here for future reference

        // mandatory
        let block_no = self.block_number.as_ref().unwrap().as_u64();
        let from = format!("{:?}", self.from);
        let to = if self.to.is_some() {
            format!("{:?}", self.to.as_ref().unwrap())
        } else {
            let zero_address = ethers::types::Address::zero();
            format!("{:?}", zero_address)
        };
        let tx_hash = format!("{:?}", self.hash);
        let input = self.input.to_string();
        let nonce = self.nonce.as_u64();
        let value = self.value.to_string();
        let gas = self.gas.as_u64();
        let r = self.r.to_string();
        let s = self.s.to_string();
        let v = self.v.as_u64();

        // optional
        let gas_price = if self.gas_price.is_some() {
            Some(self.gas_price.as_ref().unwrap().as_u64())
        } else {
            None
        };
        let bytes4 = if self.input.len() >= 4 {
            Some(input.get(2..10).unwrap())
        } else {
            None
        };
        let max_fee_per_gas = if self.max_fee_per_gas.is_some() {
            Some(self.max_fee_per_gas.as_ref().unwrap().as_u64())
        } else {
            None
        };
        let max_priority_fee_per_gas = if self.max_priority_fee_per_gas.is_some() {
            Some(self.max_priority_fee_per_gas.as_ref().unwrap().as_u64())
        } else {
            None
        };

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
              var(func: eq(Account.address, "{from}")) {{
                From as uid
              }}
              var(func: eq(Account.address, "{to}")) {{
                To as uid
              }}
            }}
        "#,
            block_no = block_no,
            tx_hash = tx_hash,
            from = from,
            to = to
        );

        // Mutation part of the upsert
        let mut set = format!(
            r#"
            uid(Block) <Block.number> "{block_no}" .
            uid(From) <Account.address> "{from}" .
            uid(To) <Account.address> "{to}" .

            uid(Tx) <dgraph.type> "Transaction" .
            uid(Tx) <Transaction.hash> "{tx_hash}" .
            uid(Tx) <Transaction.block> uid(Block) .
            uid(Tx) <Transaction.from> uid(From) .
            uid(Tx) <Transaction.to> uid(To) .
            uid(Tx) <Transaction.input> "{input}" .
            uid(Tx) <Transaction.nonce> "{nonce}" .
            uid(Tx) <Transaction.value> "{value}" .
            uid(Tx) <Transaction.gas> "{gas}" .
            uid(Tx) <Transaction.r> "{r}" .
            uid(Tx) <Transaction.s> "{s}" .
            uid(Tx) <Transaction.v> "{v}" .

        "#,
            block_no = self.block_number.unwrap().as_u64(),
            tx_hash = tx_hash,
            from = from,
            to = to,
            nonce = nonce,
            input = input,
            value = value,
            gas = gas,
            r = r,
            s = s,
            v = v,
        );

        if gas_price.is_some() {
            set.push_str(&format!(
                r#"
                uid(Tx) <Transaction.gas_price> "{gas_price}" .
            "#,
                gas_price = gas_price.unwrap()
            ));
        }

        if bytes4.is_some() {
            set.push_str(&format!(
                r#"
                uid(Tx) <Transaction.bytes4> "{bytes4}" .
            "#,
                bytes4 = bytes4.unwrap()
            ));
        }

        if max_fee_per_gas.is_some() {
            set.push_str(&format!(
                r#"
                uid(Tx) <Transaction.max_fee_per_gas> "{max_fee_per_gas}" .
            "#,
                max_fee_per_gas = max_fee_per_gas.unwrap()
            ));
        }

        if max_priority_fee_per_gas.is_some() {
            set.push_str(&format!(
                r#"
                uid(Tx) <Transaction.max_priority_fee_per_gas> "{max_priority_fee_per_gas}" .
            "#,
                max_priority_fee_per_gas = max_priority_fee_per_gas.unwrap()
            ));
        }

        // Perform the upsert
        let mut mu = dgraph_tonic::Mutation::new();
        mu.set_set_nquads(set);
        dgraph_mut_tx.upsert(query, mu).await?;

        Ok(())
    }

    fn serialize_dgraph<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Uid {
            uid: String,
        }
        #[derive(Serialize)]
        struct AddressReference {
            uid: String,
            #[serde(rename = "dgraph.type")]
            _type: String,
            #[serde(rename = "Account.address")]
            address: String,
        }
        let input = self.input.to_string();
        let bytes4 = if self.input.len() >= 4 {
            Some(input.get(2..10).unwrap())
        } else {
            None
        };
        let mut state = serializer.serialize_struct("Transaction", 14)?;
        state.serialize_field("dgraph.type", "Transaction")?;
        state.serialize_field("uid", &format!("_:{:?}", self.hash))?;
        state.serialize_field("Transaction.hash", &format!("{:?}", self.hash))?;
        state.serialize_field(
            "Transaction.from",
            &AddressReference {
                uid: format!("_:{:?}", self.from),
                _type: "Account".to_string(),
                address: format!("{:?}", self.from),
            },
        )?;
        if self.to.is_some() {
            let to = self.to.as_ref().unwrap();
            state.serialize_field(
                "Transaction.to",
                &AddressReference {
                    uid: format!("_:{:?}", to),
                    _type: "Account".to_string(),
                    address: format!("{:?}", to),
                },
            )?;
        } else {
            let zero_address = ethers::types::Address::zero();
            state.serialize_field(
                "Transaction.to",
                &AddressReference {
                    uid: format!("_:{:?}", zero_address),
                    _type: "Account".to_string(),
                    address: format!("{:?}", zero_address),
                },
            )?;
        }
        state.serialize_field(
            "Transaction.block",
            &Uid {
                uid: format!("_:{}", self.block_number.as_ref().unwrap().as_u64()),
            },
        )?;
        state.serialize_field("Transaction.value", &self.value.to_string())?;
        state.serialize_field("Transaction.gas", &self.gas.as_u64())?;
        if self.gas_price.is_some() {
            state.serialize_field(
                "Transaction.gas_price",
                &self.gas_price.as_ref().unwrap().as_u64(),
            )?;
        }
        state.serialize_field("Transaction.input", &input)?;
        if bytes4.is_some() {
            state.serialize_field("Transaction.bytes4", bytes4.as_ref().unwrap())?;
        }
        if self.max_fee_per_gas.is_some() {
            state.serialize_field(
                "Transaction.max_fee_per_gas",
                &self.max_fee_per_gas.as_ref().unwrap().as_u64(),
            )?;
        }
        if self.max_priority_fee_per_gas.is_some() {
            state.serialize_field(
                "Transaction.max_priority_fee_per_gas",
                &self.max_priority_fee_per_gas.as_ref().unwrap().as_u64(),
            )?;
        }
        state.serialize_field("Transaction.nonce", &self.nonce.as_u64())?;
        state.serialize_field("Transaction.r", &self.r.to_string())?;
        state.serialize_field("Transaction.s", &self.s.to_string())?;
        state.serialize_field("Transaction.v", &self.v.to_string())?;
        state.end()
    }
}

impl SerializeDgraph for Transaction {
    fn serialize_dgraph<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.serialize_dgraph(serializer)
    }
}

#[cfg(test)]
mod tests {
    use dgraph_tonic::Mutate;
    use ethers::providers::{Middleware, Provider};
    use std::{str::FromStr, sync::Arc};

    #[tokio::test]
    async fn transction_serialization() {
        let eth_node = std::env::var("ETH_NODE").expect("ETH_NODE env var is not set");

        let eth_client = Arc::new(Provider::try_from(eth_node).unwrap());

        let tx = ethers::types::TxHash::from_str(
            "0x4163e5d06aa6d974b0898a6fa89473516716ade2c38d90d1b20bb814a69a6fb1",
        )
        .unwrap();

        let tx = eth_client.get_transaction(tx).await.unwrap();

        let tx = super::Transaction::from(tx.unwrap());

        let mut serializer = serde_json::Serializer::new(Vec::new());
        tx.serialize_dgraph(&mut serializer).unwrap();
        println!("{}", String::from_utf8(serializer.into_inner()).unwrap());
    }

    #[tokio::test]
    #[ignore]
    async fn test_upsert() {
        let dgraph_endpoint = std::env::var("DGRAPH").expect("Dgraph endpoint");
        let eth_endpoint = std::env::var("ETH_NODE").expect("Ethereum endpoint");
        println!("Connecting to dgraph at {}", dgraph_endpoint);
        println!("Connecting to eth at {}", eth_endpoint);

        let eth_client = Arc::new(Provider::try_from(eth_endpoint).unwrap());
        let dgraph = dgraph_tonic::Client::new(dgraph_endpoint.clone()).expect("Dgraph client");

        let tx = ethers::types::TxHash::from_str(
            "0xfa3ea78931107e82698f3aa377ce30c6aaad8ea626e13b794489410a33350020",
        )
        .unwrap();

        let tx = eth_client.get_transaction(tx).await.unwrap();

        let tx = super::Transaction::from(tx.unwrap());

        let now = tokio::time::Instant::now();

        tx.upsert(&dgraph).await.expect("Upsert failed");

        let elapsed = now.elapsed();

        println!("Transaction upsert took {:?}", elapsed);
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

        let with_tx = eth_client
            .get_block_with_txs(ethers::types::U64::from(16100001))
            .await
            .unwrap()
            .unwrap();

        let now = tokio::time::Instant::now();

        for tx in with_tx.transactions {
            let tx = super::Transaction::from(tx);
            tx.upsert(&dgraph).await.expect("Upsert failed");
        }

        let elapsed = now.elapsed();

        println!("Block tx upsert took {:?}", elapsed);
    }

    #[tokio::test]
    #[ignore]
    async fn test_upsert_baking() {
        let dgraph_endpoint = std::env::var("DGRAPH").expect("Dgraph endpoint");
        let eth_endpoint = std::env::var("ETH_NODE").expect("Ethereum endpoint");
        println!("Connecting to dgraph at {}", dgraph_endpoint);
        println!("Connecting to eth at {}", eth_endpoint);

        let eth_client = Arc::new(Provider::try_from(eth_endpoint).unwrap());
        let dgraph = dgraph_tonic::Client::new(dgraph_endpoint.clone()).expect("Dgraph client");

        let with_tx = eth_client
            .get_block_with_txs(ethers::types::U64::from(16000010))
            .await
            .unwrap()
            .unwrap();

        let mut dgraph_mut_tx = dgraph.new_mutated_txn();

        let now = tokio::time::Instant::now();

        for tx in with_tx.transactions {
            let tx = super::Transaction::from(tx);
            tx._bake_tx(&mut dgraph_mut_tx)
                .await
                .expect("Baking failed");
        }

        dgraph_mut_tx.commit().await.expect("Commit failed");

        let elapsed = now.elapsed();

        println!("Block tx upsert took {:?}", elapsed);
    }
}
