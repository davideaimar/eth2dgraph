use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crate::utils::decompile::decompile;
use crate::{
    extraction::logs::get_transfer_from_logs,
    models::{
        block::Block, contract_destruction::ContractDestruction, skeleton::Skeleton, trace::Traces,
    },
    StreamDgraphArgs,
};
use dgraph_tonic::{Client, ClientVariant, IClient, Query};
use ethabi::ethereum_types::U64;
use ethers::providers::{Middleware, Ws};
use futures::StreamExt;
use serde::Deserialize;
use tokio::sync::Semaphore;

#[derive(Debug)]
pub enum LiveBlockErr {
    BlockNotAvailable,
    NetworkError,
    DgraphError,
}

pub async fn process_live_block<T: Middleware + 'static, S: IClient>(
    block_n: u64,
    eth_node: Arc<T>,
    dgraph: Arc<ClientVariant<S>>,
    args: Arc<StreamDgraphArgs>,
) -> Result<(), LiveBlockErr> {
    let now = tokio::time::Instant::now();
    let with_tx = eth_node.get_block_with_txs(block_n);
    // filter logs by block number
    let filter = ethers::core::types::Filter::default()
        .from_block(block_n)
        .to_block(block_n);
    let logs = eth_node.get_logs(&filter);
    let traces = eth_node.trace_block(ethers::types::BlockNumber::Number(U64::from(block_n)));

    let (with_tx, logs, traces) = tokio::join!(with_tx, logs, traces);

    let with_tx = with_tx
        .map_err(|_| LiveBlockErr::NetworkError)?
        .ok_or(LiveBlockErr::BlockNotAvailable)?;

    let logs = logs.map_err(|_| LiveBlockErr::NetworkError)?;

    let traces = traces.map_err(|_| LiveBlockErr::NetworkError)?;
    let traces = Traces::from(traces);

    let destructions: Vec<ContractDestruction> = Vec::from(&traces);
    let deployments = Vec::from(traces);

    let stats = (
        block_n,
        with_tx.transactions.len(),
        logs.len(),
        deployments.len(),
        destructions.len(),
    );

    let block = Block::from(with_tx.clone());
    block
        .upsert(&dgraph)
        .await
        .map_err(|_| LiveBlockErr::DgraphError)?;

    if args.include_tokens {
        let res = crate::models::block::Block::upsert_delete_transfers(
            block.number.as_ref().unwrap().as_u64(),
            &dgraph,
        )
        .await;
        match res {
            Ok(_) => {
                let transfers = get_transfer_from_logs(&logs);
                for transfer in transfers {
                    let res = transfer.upsert(&dgraph).await;
                    if let Err(_) = res {
                        println!("Error upserting transfer: {:?}", transfer);
                        println!("Continuing...");
                    }
                }
            }
            Err(_) => {
                println!(
                    "Error deleting transfers for block {}",
                    block.number.as_ref().unwrap().as_u64()
                );
                println!("Continue skipping storing transfers...");
            }
        }
    }

    if args.include_logs {
        let res = crate::models::block::Block::upsert_delete_logs(
            block.number.as_ref().unwrap().as_u64(),
            &dgraph,
        )
        .await;
        match res {
            Ok(_) => {
                for log in logs {
                    let log = crate::models::log::Log::from(log);
                    let res = log.upsert(&dgraph).await;
                    if let Err(_) = res {
                        println!("Error upserting log: {:?}", log);
                        println!("Continuing...");
                    }
                }
            }
            Err(_) => {
                println!(
                    "Error deleting logs for block {}",
                    block.number.as_ref().unwrap().as_u64()
                );
                println!("Continue skipping storing logs...");
            }
        }
    }

    if args.include_tx {
        for tx in with_tx.transactions {
            let tx = crate::models::transaction::Transaction::from(tx);
            let res = tx.upsert(&dgraph).await;
            if let Err(_) = res {
                println!("Error upserting tx: {:?}", tx);
                println!("Continuing...");
            }
        }
    }

    let res = crate::models::block::Block::upsert_delete_destructions(
        block.number.as_ref().unwrap().as_u64(),
        &dgraph,
    )
    .await;
    match res {
        Ok(_) => {
            for destruction in destructions {
                let res = destruction.upsert(&dgraph).await;
                if let Err(_) = res {
                    println!("Error upserting destruction: {:?}", destruction);
                    println!("Continuing...");
                }
            }
        }
        Err(_) => {
            println!(
                "Error deleting destructions for block {}",
                block.number.as_ref().unwrap().as_u64()
            );
            println!("Continue skipping storing destructions...");
        }
    }

    let res = crate::models::block::Block::upsert_delete_deployments(
        block.number.as_ref().unwrap().as_u64(),
        &dgraph,
    )
    .await;
    match res {
        Ok(_) => {
            for deployment in deployments {
                // if args.scs_path.is_some() {
                //     deployment.check_verification(args.scs_path.as_ref().unwrap());
                // }

                // Steps:
                // 1: check if the skeleton already exists
                //   If not:
                //     1.1: decompile the skeleton
                //     1.2: upsert the skeleton
                //     1.3: return uid
                //   If yes:
                //     1.1: return uid
                // 2: upsert the deployment, using the skeleton uid

                let skeleton = deployment.skeleton();

                // 1: check if the skeleton already exists
                let query = r#"
                    query skeleton($skeleton: string)  {
                        skeleton(func: eq(Skeleton.bytecode, $skeleton)) {
                            uid
                        }
                    }
                "#;

                #[derive(Deserialize, Debug)]
                struct QueryItem {
                    uid: String,
                }

                #[derive(Deserialize, Debug)]
                struct QueryResult {
                    skeleton: Vec<QueryItem>,
                }

                let mut vars = HashMap::new();
                vars.insert("$skeleton", format!("{}", skeleton));

                let mut txn = dgraph.new_read_only_txn();
                let res = txn
                    .query_with_vars(query, vars)
                    .await
                    .map_err(|_| LiveBlockErr::DgraphError)?;

                let res: QueryResult = serde_json::from_slice(&res.json)
                    .expect("Error while deserializing the query result");

                let skeleton_uid = if !res.skeleton.is_empty() {
                    // 1.1: the skeleton already exists
                    res.skeleton.get(0).unwrap().uid.clone()
                } else {
                    // 1.1: decompile the skeleton
                    let decompiled_skeleton = decompile(
                        &deployment.contract_address(),
                        &deployment.deployed_code(),
                        args.decompiler_timeout,
                    )
                    .await;

                    let mut skeleton = Skeleton::new(deployment.skeleton().clone());

                    match decompiled_skeleton {
                        Ok(decompiled_skeleton) => {
                            skeleton.set_abi(decompiled_skeleton);
                        }
                        Err(_) => {
                            skeleton.set_failed_decompilation(true);
                        }
                    }

                    // 1.2: Insert the skeleton, getting the uid
                    match skeleton.upsert(&dgraph).await {
                        Ok(uid) => uid,
                        Err(_) => {
                            println!("Error upserting skeleton: {:?}", skeleton);
                            println!("Continuing...");
                            continue;
                        }
                    }
                };

                // 2: upsert the deployment, using the skeleton uid
                let res = deployment.upsert(&skeleton_uid, &dgraph).await;
                if let Err(e) = res {
                    println!("Error upserting deployment: {:?}", e);
                    println!("Continuing...");
                }
            }
        }
        Err(_) => {
            println!(
                "Error deleting deployments for block {}",
                block.number.as_ref().unwrap().as_u64()
            );
            println!("Continue skipping storing deployments...");
        }
    }
    let elapsed = now.elapsed();
    println!(
        "Procesed block {} in {}s, stats: {:?}",
        block_n,
        elapsed.as_secs_f32(),
        stats
    );

    Ok(())
}

pub async fn sync_to_live<T: Middleware + 'static, S: IClient + 'static>(
    args: Arc<StreamDgraphArgs>,
    eth_node: Arc<T>,
    dgraph_client: Arc<ClientVariant<S>>,
) {
    let num_jobs = args.num_jobs;
    println!("Starting sync to live with {} threads", num_jobs);
    // get last indexed block in Dgraph
    let mut txn = dgraph_client.new_read_only_txn();
    let query = r#"{
        last_block(func: has(Block.number), orderdesc: Block.number, first: 1) {
          b: Block.number
      }
    }"#;
    let resp = txn.query(query).await.expect("Query failed");
    #[derive(serde::Deserialize, Debug)]
    struct QueryItem {
        b: u64,
    }
    #[derive(serde::Deserialize, Debug)]
    struct QueryResult {
        last_block: Vec<QueryItem>,
    }
    let last_block: QueryResult =
        serde_json::from_slice(&resp.json).expect("Could not parse last block");
    let last_block = last_block.last_block.get(0).unwrap().b;
    println!("Last block in Dgraph: {}", last_block);
    println!("Syncing to live chain...");
    let semaphore = Arc::new(Semaphore::new(num_jobs));
    let done = Arc::new(AtomicBool::new(false));
    let curr_block = Arc::new(AtomicU64::new(last_block + 1));
    while !done.load(Ordering::Relaxed) {
        let a = args.clone();
        let eth = eth_node.clone();
        let dgraph = dgraph_client.clone();
        let d = done.clone();
        let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
        let block_no = curr_block.clone();
        tokio::spawn(async move {
            let curr_block = block_no.fetch_add(1, Ordering::Relaxed);
            match process_live_block(curr_block, eth, dgraph, a).await {
                Ok(_) => {}
                Err(e) => match e {
                    LiveBlockErr::BlockNotAvailable => {
                        println!("Block {} not available yet", curr_block);
                        println!("Quitting sync...");
                        d.store(true, Ordering::Relaxed);
                    }
                    LiveBlockErr::NetworkError => {
                        println!("Network error, retrying");
                    }
                    LiveBlockErr::DgraphError => {
                        println!("Dgraph error, retrying");
                    }
                },
            };
            drop(permit); // release the permit
        });
    }

    let _ = semaphore.acquire_many(num_jobs as u32).await;
}

pub async fn run_stream_extraction(args: StreamDgraphArgs) {
    println!("Running stream extraction");
    println!("Args: {:?}", args);

    let args = Arc::new(args);

    let ws = Ws::connect(&args.endpoint)
        .await
        .expect("Could not connect to ws");
    let eth_provider = Arc::new(ethers::providers::Provider::new(ws));
    let dgraph_client = Arc::new(Client::new(&args.dgraph).expect("Dgraph client"));

    if !args.no_sync {
        // sync Dgraph with last available block
        let a = args.clone();
        let eth = eth_provider.clone();
        let dgraph = dgraph_client.clone();
        sync_to_live(a, eth, dgraph).await;
    }

    println!("Starting stream extraction");

    let mut stream = eth_provider
        .subscribe_blocks()
        .await
        .expect("Could not subscribe to blocks");

    while let Some(block) = stream.next().await {
        let block_n = block.number.unwrap().as_u64();
        let a = args.clone();
        let eth = eth_provider.clone();
        let dgraph = dgraph_client.clone();
        process_live_block(block_n, eth, dgraph, a)
            .await
            .expect("Could not process block");
    }

    println!("Finished stream extraction");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_not_available_block() {
        let dgraph_endpoint = std::env::var("DGRAPH").expect("Dgraph endpoint");
        let eth_endpoint = std::env::var("ETH_NODE").expect("Ethereum endpoint");
        println!("Connecting to dgraph at {}", dgraph_endpoint);
        println!("Connecting to eth at {}", eth_endpoint);
        let ws = Ws::connect(eth_endpoint.clone())
            .await
            .expect("Could not connect to dgraph");
        let provider = Arc::new(ethers::providers::Provider::new(ws));
        let dgraph = Arc::new(Client::new(dgraph_endpoint.clone()).expect("Dgraph client"));
        let args = StreamDgraphArgs {
            endpoint: eth_endpoint,
            dgraph: dgraph_endpoint,
            include_tx: false,
            include_tokens: false,
            include_logs: false,
            decompiler_timeout: 5000,
            no_sync: false,
            num_jobs: 1,
        };
        let args = Arc::new(args);
        let res = process_live_block(190000000, provider, dgraph, args).await;
        match res {
            Ok(_) => panic!("Block should not be available"),
            Err(e) => match e {
                LiveBlockErr::BlockNotAvailable => {}
                _ => panic!("Wrong error"),
            },
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_live_block_process() {
        let _block_no = 16100010;

        let dgraph_endpoint = std::env::var("DGRAPH").expect("Dgraph endpoint");
        let eth_endpoint = std::env::var("ETH_NODE").expect("Ethereum endpoint");
        let ws = Ws::connect(eth_endpoint.clone())
            .await
            .expect("Could not connect to dgraph");
        let _provider = ethers::providers::Provider::new(ws);
        let _dgraph = Client::new(dgraph_endpoint.clone()).expect("Dgraph client");
        let _args = StreamDgraphArgs {
            endpoint: eth_endpoint,
            dgraph: dgraph_endpoint,
            include_tx: true,
            include_tokens: true,
            include_logs: true,
            decompiler_timeout: 5000,
            no_sync: true,
            num_jobs: 1,
        };
        // let args = Rc::new(args);
        // process_live_block(block_no, &provider, &dgraph, args)
        //     .await
        //     .unwrap();
    }
}
