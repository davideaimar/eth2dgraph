use dgraph_tonic::{IClient, Mutate};
use ethers::types::{Address, TxHash, U256, U64};
use serde::{ser::SerializeStruct, Serialize, Serializer};
use serde_json::json;

use super::SerializeDgraph;

#[derive(Debug)]
pub enum TokenType {
    ERC20,
    ERC721,
}

#[derive(Debug)]
pub struct TokenTransfer {
    contract: Address,
    from: Address,
    to: Address,
    value: U256,
    block: U64,
    tx_hash: TxHash,
    token_type: TokenType,
}

impl TokenTransfer {
    pub fn new(
        contract: Address,
        from: Address,
        to: Address,
        value: U256,
        block: U64,
        tx_hash: TxHash,
        token_type: TokenType,
    ) -> Self {
        Self {
            contract,
            from,
            to,
            value,
            block,
            tx_hash,
            token_type,
        }
    }

    pub async fn upsert<S: IClient>(
        &self,
        dgraph_client: &dgraph_tonic::ClientVariant<S>,
    ) -> Result<(), anyhow::Error> {
        // WARNING:
        // Token transfers don't have a unique identifier
        // upserting already existing transfers will result in a duplicate
        // This function should be called just after checking if the transfer
        // of a certain block already exists, or after deleting them using Block::upsert_delete_transfers

        let block_no = self.block.as_u64();
        let contract_address = format!("{:?}", self.contract);
        let tx_hash = format!("{:?}", self.tx_hash);
        let from = format!("{:?}", self.from);
        let to = format!("{:?}", self.to);
        let value = format!("{}", self.value);

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
            contract_address = contract_address,
            from = from,
            to = to
        );

        // Mutation part of the upsert
        let set = format!(
            r#"
            uid(Block) <Block.number> "{block_no}" .
            uid(Block) <dgraph.type> "Block" .
            uid(From) <Account.address> "{from}" .
            uid(From) <dgraph.type> "Account" .
            uid(To) <Account.address> "{to}" .
            uid(To) <dgraph.type> "Account" .
            uid(Tx) <Transaction.hash> "{tx_hash}" .
            uid(Tx) <dgraph.type> "Transaction" .
            uid(Contract) <Account.address> "{contract_address}" .
            uid(Contract) <dgraph.type> "Account" .
            uid(Contract) <Account.is_contract> "true" .
            _:transfer <dgraph.type> "TokenTransfer" .
            _:transfer <TokenTransfer.block> uid(Block) .
            _:transfer <TokenTransfer.tx> uid(Tx) .
            _:transfer <TokenTransfer.contract> uid(Contract) .
            _:transfer <TokenTransfer.from> uid(From) .
            _:transfer <TokenTransfer.to> uid(To) .
            _:transfer <TokenTransfer.value> "{value}" .
        "#,
            block_no = block_no,
            contract_address = contract_address,
            value = value,
            from = from,
            to = to,
            tx_hash = tx_hash
        );

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
        #[derive(Serialize)]
        struct Uid {
            uid: String,
        }
        #[derive(Serialize)]
        struct TransactionReference {
            uid: String,
            #[serde(rename = "dgraph.type")]
            _type: String,
            #[serde(rename = "Transaction.hash")]
            hash: String,
        }
        #[derive(Serialize)]
        struct AddressReference {
            uid: String,
            #[serde(rename = "dgraph.type")]
            _type: String,
            #[serde(rename = "Account.address")]
            address: String,
        }
        let mut state = serializer.serialize_struct("TokenTransfer", 7)?;
        state.serialize_field("dgraph.type", "TokenTransfer")?;
        state.serialize_field(
            "TokenTransfer.contract",
            &json!({
                "uid": format!("_:{:?}", self.contract),
                "dgraph.type": "Account",
                "Account.address": format!("{:?}", self.contract),
                "Account.is_contract": true,
            }),
        )?;
        state.serialize_field(
            "TokenTransfer.from",
            &AddressReference {
                uid: format!("_:{:?}", self.from),
                _type: "Account".to_string(),
                address: format!("{:?}", self.from),
            },
        )?;
        state.serialize_field(
            "TokenTransfer.to",
            &AddressReference {
                uid: format!("_:{:?}", self.to),
                _type: "Account".to_string(),
                address: format!("{:?}", self.to),
            },
        )?;
        match self.token_type {
            TokenType::ERC20 => {
                state.serialize_field("TokenTransfer.value", &format!("{}", self.value))?;
            }
            TokenType::ERC721 => {
                state.serialize_field("TokenTransfer.token_id", &format!("{}", self.value))?;
            }
        }
        state.serialize_field(
            "TokenTransfer.block",
            &Uid {
                uid: format!("_:{}", self.block.as_u64()),
            },
        )?;
        state.serialize_field(
            "TokenTransfer.tx",
            &Uid {
                uid: format!("_:{:?}", self.tx_hash),
            },
        )?;
        state.end()
    }
}

impl SerializeDgraph for TokenTransfer {
    fn serialize_dgraph<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.serialize_dgraph(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        extraction::logs::{get_all_logs, get_transfer_from_logs},
        models::block::Block,
    };
    use ethers::{
        providers::Provider,
        types::{Address, TxHash, U256, U64},
    };
    use serde_json::json;
    use std::str::FromStr;
    use std::sync::Arc;

    #[test]
    fn test_transfer_serialization() {
        let transfer = TokenTransfer::new(
            Address::from_low_u64_be(1),
            Address::from_low_u64_be(2),
            Address::from_low_u64_be(3),
            U256::from_str("0x0000000000000000000000000000000000000000000000000000000000000001")
                .unwrap(),
            U64::from(5),
            TxHash::from_str("0x1844fe0131ddb020be1764d1c28f0ae03335a9d1b1348fb8c13d84a279c4a955")
                .unwrap(),
            TokenType::ERC20,
        );
        let expected = json!({
            "dgraph.type": "TokenTransfer",
            "TokenTransfer.contract": {
                "uid": "_:0x0000000000000000000000000000000000000001",
                "dgraph.type": "Account",
                "Account.address": "0x0000000000000000000000000000000000000001",
                "Account.is_contract": true
            },
            "TokenTransfer.from": {
                "uid": "_:0x0000000000000000000000000000000000000002",
                "dgraph.type": "Account",
                "Account.address": "0x0000000000000000000000000000000000000002"
            },
            "TokenTransfer.to": {
                "uid": "_:0x0000000000000000000000000000000000000003",
                "dgraph.type": "Account",
                "Account.address": "0x0000000000000000000000000000000000000003"
            },
            "TokenTransfer.value": "1",
            "TokenTransfer.block": {
                "uid": "_:5"
            },
            "TokenTransfer.tx": {
                "uid": "_:0x1844fe0131ddb020be1764d1c28f0ae03335a9d1b1348fb8c13d84a279c4a955"
            }
        })
        .to_string();

        let mut serializer = serde_json::Serializer::new(Vec::new());
        transfer.serialize_dgraph(&mut serializer).unwrap();
        let serialized = String::from_utf8(serializer.into_inner()).unwrap();

        assert_eq!(serialized, expected);
    }

    #[tokio::test]
    #[ignore]
    async fn test_transfer_serialization_in_block() {
        let eth_endpoint = std::env::var("ETH_NODE").expect("Ethereum endpoint");
        println!("Connecting to eth at {}", eth_endpoint);

        let eth_client = Arc::new(Provider::try_from(eth_endpoint).unwrap());

        let block = 16100001u64;

        let logs = get_all_logs(block, eth_client).await.unwrap();
        let transfers: Vec<TokenTransfer> = get_transfer_from_logs(&logs);

        for transfer in transfers {
            let mut serializer = serde_json::Serializer::new(Vec::new());
            transfer.serialize_dgraph(&mut serializer).unwrap();
            let serialized = String::from_utf8(serializer.into_inner()).unwrap();
            println!("{}", serialized);
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_transfer_upsert() {
        let dgraph_endpoint = std::env::var("DGRAPH").expect("Dgraph endpoint");
        let eth_endpoint = std::env::var("ETH_NODE").expect("Ethereum endpoint");
        println!("Connecting to dgraph at {}", dgraph_endpoint);
        println!("Connecting to eth at {}", eth_endpoint);

        let eth_client = Arc::new(Provider::try_from(eth_endpoint).unwrap());
        let dgraph = dgraph_tonic::Client::new(dgraph_endpoint.clone()).expect("Dgraph client");

        let block = 16100001u64;

        let logs = get_all_logs(block, eth_client).await.unwrap();
        let transfers: Vec<TokenTransfer> = get_transfer_from_logs(&logs);

        let transfer_to_test = transfers.get(0).unwrap();
        transfer_to_test.upsert(&dgraph).await.unwrap();
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
        let transfers: Vec<TokenTransfer> = get_transfer_from_logs(&logs);

        let now = tokio::time::Instant::now();

        Block::upsert_delete_transfers(block_no, &dgraph)
            .await
            .expect("Delete upsert failed");

        for transfer in transfers {
            transfer.upsert(&dgraph).await.expect("Set upsert failed");
        }

        let elapsed = now.elapsed();

        println!("Block transfers upsert took {:?}", elapsed);
    }
}
