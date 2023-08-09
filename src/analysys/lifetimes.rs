//! This analysys aim to understand the lifetimes of the smart contracts.
//! With lifetime I mean the time between the creation of the contract and its destruction.
//! A contract can be deployed and then destroyed multiple times,
//! this is a case that will be also analyzed.
//!
//! RQ1: How many contracts have been destroyed and how many have not?
//! RQ2: How many contracts have been deployed and destroyed multiple times and how many only once?
//! RQ3: How many contracts have been deployed and destroyed in the same block but in different transactions? And how many in the same transaction?
//! RQ4: Of the contracts that have been destroyed, for how long do they live?

use bincode::{deserialize_from, serialize_into};
use chrono::DateTime;
use dgraph_tonic::Client;
use futures::pin_mut;
use futures::stream::StreamExt;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::io::{BufReader, BufWriter, Read};

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Block {
    #[serde(rename = "n")]
    number: u64,
    #[serde(rename = "d")]
    datetime: String, // as ISO 8601
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LifeEvent {
    #[serde(rename = "tx")]
    tx_hash: String,
    #[serde(rename = "b")]
    block: Block,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ContractLife {
    uid: String,
    #[serde(rename = "dp")]
    deploys: Option<Vec<LifeEvent>>,
    #[serde(rename = "de")]
    destructions: Option<Vec<LifeEvent>>,
}

fn write_binary<T>(data: &T, file: &str) -> Result<(), std::io::Error>
where
    T: Serialize,
{
    let mut f = BufWriter::new(File::create(file)?);
    serialize_into(&mut f, data).unwrap();
    Ok(())
}

fn load_binary(file: &str) -> Result<Vec<ContractLife>, std::io::Error> {
    let mut buf_stream_reader = BufReader::new(File::open(file).unwrap());
    let mut data = Vec::new();
    buf_stream_reader.read_to_end(&mut data).unwrap();
    let cursor = &data[..];
    let data: Vec<ContractLife> = deserialize_from(cursor).unwrap();
    Ok(data)
}

fn rq_1(data: &Vec<ContractLife>, writer: &mut BufWriter<File>) {
    writeln!(
        writer,
        "### RQ1: How many contracts have been destroyed and how many have not? ###"
    )
    .unwrap();
    let destroyed = data
        .par_iter()
        .filter(|c| c.destructions.is_some() && c.destructions.as_ref().unwrap().len() > 0)
        .count();
    let not_destroyed = data.len() - destroyed;
    writeln!(
        writer,
        "RQ1: {} contracts have been destroyed and {} have not.",
        destroyed, not_destroyed
    )
    .unwrap();
}

fn rq_2(data: &Vec<ContractLife>, writer: &mut BufWriter<File>) {
    writeln!(writer, "### RQ2: How many contracts have been deployed and destroyed multiple times and how many only once? ###").unwrap();
    let res: (usize, usize) = data
        .par_iter()
        .fold(
            || (0, 0),
            |acc, curr| {
                if curr.destructions.is_some() && curr.destructions.as_ref().unwrap().len() > 0 {
                    if curr.destructions.as_ref().unwrap().len() == 1 {
                        (acc.0 + 1, acc.1)
                    } else {
                        (acc.0, acc.1 + 1)
                    }
                } else {
                    acc
                }
            },
        )
        .reduce(|| (0, 0), |a, b| (a.0 + b.0, a.1 + b.1));
    writeln!(writer, "RQ2: contracts that have been destroyed multiple times: {}, contracts that have been destroyed only once: {}", res.1, res.0).unwrap();
}

fn rq_3(data: &Vec<ContractLife>, writer: &mut BufWriter<File>) {
    writeln!(writer, "### RQ3: How many contracts have been deployed and destroyed in the same block but in different transactions? And how many in the same transaction? ###").unwrap();
    let ((same_block_con, same_block_tot), (same_tx_con, same_tx_tot)): (
        (usize, usize),
        (usize, usize),
    ) = data
        .par_iter()
        .fold(
            || ((0, 0), (0, 0)),
            |acc, curr| {
                // find deploys and destructions in the same block or tx
                if curr.destructions.is_some()
                    && curr.destructions.as_ref().unwrap().len() > 0
                    && curr.deploys.is_some()
                    && curr.deploys.as_ref().unwrap().len() > 0
                {
                    let mut same_block = 0;
                    let mut same_tx = 0;
                    for destruction in curr.destructions.as_ref().unwrap() {
                        for deploy in curr.deploys.as_ref().unwrap() {
                            if destruction.block.number == deploy.block.number {
                                if destruction.tx_hash == deploy.tx_hash {
                                    same_tx += 1;
                                } else {
                                    same_block += 1;
                                }
                            }
                        }
                    }
                    let same_block_con = if same_block > 0 { 1 } else { 0 };
                    let same_tx_con = if same_tx > 0 { 1 } else { 0 };
                    (
                        (acc.0 .0 + same_block_con, acc.0 .1 + same_block),
                        (acc.1 .0 + same_tx_con, acc.1 .1 + same_tx),
                    )
                } else {
                    acc
                }
            },
        )
        .reduce(
            || ((0, 0), (0, 0)),
            |a, b| {
                (
                    (a.0 .0 + b.0 .0, a.0 .1 + b.0 .1),
                    (a.1 .0 + b.1 .0, a.1 .1 + b.1 .1),
                )
            },
        );
    writeln!(writer, "RQ3: {} distinct contracts happened to have been deployed and destroyed in the same block but in different transactions, for a total of {} times.", same_block_con, same_block_tot).unwrap();
    writeln!(writer, "RQ3: {} distinct contracts happened to have been deployed and destroyed in the same transaction, for a total of {} times.", same_tx_con, same_tx_tot).unwrap();
}

fn rq_4(data: &Vec<ContractLife>, writer: &mut BufWriter<File>) {
    writeln!(
        writer,
        "### RQ4: Of the contracts that have been destroyed, for how long do they live? ###"
    )
    .unwrap();

    // let lifetimes: Vec<((u64, u64), (u64, u64))> = data.par_iter()
    //   .filter(|c| c.destructions.is_some() && c.destructions.as_ref().unwrap().len() > 0 && c.deploys.is_some() && c.deploys.as_ref().unwrap().len() > 0)
    //   .map( |c| {
    //     let max_destruction = c.destructions.as_ref().unwrap().iter().max_by_key(|d| d.block.number).unwrap();
    //     let min_deploy = c.deploys.as_ref().unwrap().iter().min_by_key(|d| d.block.number).unwrap();
    //     // merge vectors such each deploy is before the destruction
    //     let mut deploys = c.deploys.as_ref().unwrap().clone();
    //     deploys.sort_by(|a, b| a.block.number.cmp(&b.block.number));
    //     let mut destructions = c.destructions.as_ref().unwrap().clone();
    //     destructions.sort_by(|a, b| a.block.number.cmp(&b.block.number));
    //     let life = deploys.iter()
    //       .chain(destructions.iter())
    //       .collect::<Vec<&LifeEvent>>();
    //     // find the maximum number of blocks between a deploy and the subsequent destruction
    //     let max_lifetime = life.chunks_exact(2)
    //       .map(|pair| {
    //         let deploy_datetime = DateTime::parse_from_rfc3339(&pair[0].block.datetime).unwrap();
    //         let destruction_datetime = DateTime::parse_from_rfc3339(&pair[1].block.datetime).unwrap();

    //         (pair[1].block.number - pair[0].block.number, destruction_datetime.signed_duration_since(deploy_datetime).num_seconds() as u64)
    //       })
    //       .max_by_key(|l| l.0).unwrap();

    //     ((min_deploy, max_destruction), max_lifetime)
    //   })
    //   .filter(|((deploy, destruction), (_, _))| destruction.block.number > deploy.block.number)
    //   .map(|((deploy, destruction), (max_cons_block, max_cons_time))| {
    //     let block_lifetime = destruction.block.number - deploy.block.number;
    //     // from ISO 8601 string to datetime
    //     let deploy_datetime = DateTime::parse_from_rfc3339(&deploy.block.datetime).unwrap();
    //     let destruction_datetime = DateTime::parse_from_rfc3339(&destruction.block.datetime).unwrap();
    //     let date_lifetime = destruction_datetime.signed_duration_since(deploy_datetime).num_seconds() as u64;
    //     ((block_lifetime, date_lifetime), (max_cons_block, max_cons_time))
    //   })
    //   .collect();

    let lifetimes: Vec<(u64, u64)> = data
        .par_iter()
        .filter(|c| {
            c.destructions.is_some()
                && c.destructions.as_ref().unwrap().len() > 0
                && c.deploys.is_some()
                && c.deploys.as_ref().unwrap().len() > 0
        })
        .map(|l| {
            let max_destruction = l
                .destructions
                .as_ref()
                .unwrap()
                .iter()
                .max_by_key(|d| d.block.number)
                .unwrap();
            let min_deploy = l
                .deploys
                .as_ref()
                .unwrap()
                .iter()
                .min_by_key(|d| d.block.number)
                .unwrap();
            (max_destruction, min_deploy)
        })
        .filter(|(max_destruction, min_deploy)| {
            max_destruction.block.number >= min_deploy.block.number
        })
        .map(|(max_destruction, min_deploy)| {
            // from ISO 8601 string to datetime
            let deploy_datetime = DateTime::parse_from_rfc3339(&min_deploy.block.datetime).unwrap();
            let destruction_datetime =
                DateTime::parse_from_rfc3339(&max_destruction.block.datetime).unwrap();
            let date_lifetime = destruction_datetime
                .signed_duration_since(deploy_datetime)
                .num_seconds() as u64;
            let block_lifetime = max_destruction.block.number - min_deploy.block.number;
            (block_lifetime, date_lifetime)
        })
        .collect();

    //   .map( |c| {
    //     let max_destruction = c.destructions.as_ref().unwrap().iter().max_by_key(|d| d.block.number).unwrap();
    //     let min_deploy = c.deploys.as_ref().unwrap().iter().min_by_key(|d| d.block.number).unwrap();
    //     // merge vectors such each deploy is before the destruction
    //     let mut deploys = c.deploys.as_ref().unwrap().clone();
    //     deploys.sort_by(|a, b| a.block.number.cmp(&b.block.number));
    //     let mut destructions = c.destructions.as_ref().unwrap().clone();
    //     destructions.sort_by(|a, b| a.block.number.cmp(&b.block.number));
    //     let life = deploys.iter()
    //       .chain(destructions.iter())
    //       .collect::<Vec<&LifeEvent>>();
    //     // find the maximum number of blocks between a deploy and the subsequent destruction
    //     let max_lifetime = life.chunks_exact(2)
    //       .map(|pair| {
    //         let deploy_datetime = DateTime::parse_from_rfc3339(&pair[0].block.datetime).unwrap();
    //         let destruction_datetime = DateTime::parse_from_rfc3339(&pair[1].block.datetime).unwrap();

    //         (pair[1].block.number - pair[0].block.number, destruction_datetime.signed_duration_since(deploy_datetime).num_seconds() as u64)
    //       })
    //       .max_by_key(|l| l.0).unwrap();

    //     ((min_deploy, max_destruction), max_lifetime)
    //   })
    //   .filter(|((deploy, destruction), (_, _))| destruction.block.number > deploy.block.number)
    //   .map(|((deploy, destruction), (max_cons_block, max_cons_time))| {
    //     let block_lifetime = destruction.block.number - deploy.block.number;
    //     // from ISO 8601 string to datetime
    //     let deploy_datetime = DateTime::parse_from_rfc3339(&deploy.block.datetime).unwrap();
    //     let destruction_datetime = DateTime::parse_from_rfc3339(&destruction.block.datetime).unwrap();
    //     let date_lifetime = destruction_datetime.signed_duration_since(deploy_datetime).num_seconds() as u64;
    //     ((block_lifetime, date_lifetime), (max_cons_block, max_cons_time))
    //   })
    //   .collect();

    let avg_lifetime_blocks =
        lifetimes.iter().map(|l| l.0).sum::<u64>() as f64 / lifetimes.len() as f64;
    let avg_lifetime_secs =
        lifetimes.iter().map(|l| l.1).sum::<u64>() as f64 / lifetimes.len() as f64;
    writeln!(writer, "RQ4: Average lifetime of a contract considering first deploy and last destruction is {} blocks.", avg_lifetime_blocks).unwrap();
    writeln!(writer, "RQ4: Average lifetime of a contract considering first deploy and last destruction is {} seconds.", avg_lifetime_secs).unwrap();
    let std_dev = lifetimes
        .iter()
        .map(|l| (l.0 as f64 - avg_lifetime_blocks).powi(2))
        .sum::<f64>()
        / lifetimes.len() as f64;
    writeln!(
        writer,
        "RQ4: Standard deviation of lifetimes is {}.",
        std_dev.sqrt()
    )
    .unwrap();
}

pub async fn analyse_lifetimes(endpoint: &str, output_path: &str, cache_file: Option<String>) {
    if !std::path::Path::new(output_path).exists() {
        std::fs::create_dir_all(output_path).unwrap();
    }
    let mut writer = BufWriter::new(File::create(format!("{}/res.txt", output_path)).unwrap());

    let now = std::time::Instant::now();

    let contract_lives =
        if cache_file.is_some() && std::path::Path::new(cache_file.as_ref().unwrap()).exists() {
            println!("Loading data from cache file...");
            let data = load_binary(cache_file.as_ref().unwrap()).unwrap();
            println!(
                "Loaded {} contracts from cache in {:?}",
                data.len(),
                now.elapsed()
            );
            data
        } else {
            writeln!(
                &mut writer,
                "Cache file not found, starting extraction from dgraph."
            )
            .unwrap();
            let query = r#"query stream($first: string, $offset: string) {
      items(func: type(Contract), first: $first, offset: $offset) {
          uid
          dp: ~ContractDeployment.contract{
            tx: ContractDeployment.tx_hash
            b: ContractDeployment.block{
              n: Block.number
              d: Block.datetime
            }
          }
          de: ~ContractDestruction.contract{
            tx: ContractDestruction.tx_hash
            b: ContractDestruction.block{
              n: Block.number
              d: Block.datetime
            }
          }
      }
    }"#;
            let client = Client::new(endpoint).expect("Dgraph client");
            let stream = client
                .new_read_only_txn()
                .into_stream::<&str, ContractLife>(query, 1000000);
            pin_mut!(stream);
            let mut contract_lives: Vec<ContractLife> = Vec::new();
            while let Some(contract_life) = stream.next().await {
                match contract_life {
                    Ok(contract_life) => {
                        contract_lives.push(contract_life);
                    }
                    Err(e) => {
                        writeln!(&mut writer, "Error: {:?}", e).unwrap();
                    }
                }
                if contract_lives.len() % 1000000 == 0 {
                    writeln!(&mut writer, "Loaded {} contracts.", contract_lives.len()).unwrap();
                }
            }
            if cache_file.is_some() {
                // store data in binary file
                writeln!(&mut writer, "Storing data in binary file...").unwrap();
                write_binary(&contract_lives, cache_file.as_ref().unwrap()).unwrap();
                writeln!(&mut writer, "Data stored in binary file.").unwrap();
            }
            writeln!(
                &mut writer,
                "Loaded data from Dgraph in {:?}",
                now.elapsed()
            )
            .unwrap();
            contract_lives
        };

    println!("Number of contracts: {}", contract_lives.len());

    let (send, recv) = tokio::sync::oneshot::channel();

    rayon::spawn(move || {
        rq_1(&contract_lives, &mut writer);
        rq_2(&contract_lives, &mut writer);
        rq_3(&contract_lives, &mut writer);
        rq_4(&contract_lives, &mut writer);
        writer.flush().unwrap();

        send.send(()).unwrap();
    });
    // Wait for the rayon task.
    recv.await.expect("Panic in rayon::spawn");
}
