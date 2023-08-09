use crate::models::log::Log;
use crate::models::{
    abi::ABIStructure, block::Block, contract_deployment::ContractDeployment,
    contract_destruction::ContractDestruction, error::ErrorABI, event::EventABI,
    function::FunctionABI, skeleton::Skeleton, transaction::Transaction, transfer::TokenTransfer,
    SerializeDgraph,
};
use flate2::Compression;
use primitive_types::H256;
use std::mem::size_of_val;
use std::{collections::HashSet, io::Write};
use tokio::sync::mpsc::Receiver;
use tokio::time::Instant;

#[derive(Debug)]
pub enum WriteCommand {
    Block(Block),
    Transfer(TokenTransfer),
    Transaction(Transaction),
    ContractDeployment(ContractDeployment),
    ContractDestruction(ContractDestruction),
    Skeleton(Skeleton),
    Log(Log),
}

pub fn flush<T>(vec: &Vec<T>, output_file: &str, compression_level: u32)
where
    T: SerializeDgraph,
{
    let mut json: Vec<u8> = Vec::new();
    json.push(b'[');
    for item in vec {
        let mut serializer = serde_json::Serializer::new(Vec::new());
        item.serialize_dgraph(&mut serializer).unwrap();
        json.append(&mut serializer.into_inner());
        json.push(b',');
    }
    if json.len() > 1 {
        json.pop();
    }
    json.push(b']');
    let mut encoder = flate2::write::GzEncoder::new(
        std::fs::File::create(output_file).unwrap(),
        Compression::new(compression_level),
    );
    encoder.write_all(&json).unwrap();
    encoder.finish().unwrap();
}

pub async fn writer_task(
    output_path: &str,
    mut receiver: Receiver<WriteCommand>,
    output_size_kb: usize,
    compression_level: u32,
) {
    let mut stored_function_signatures: HashSet<H256> = HashSet::new();
    let mut stored_event_signatures: HashSet<H256> = HashSet::new();
    let mut stored_error_signatures: HashSet<H256> = HashSet::new();

    let mut skeletons: Vec<Skeleton> = Vec::new();
    let mut transfers: Vec<TokenTransfer> = Vec::new();
    let mut events: Vec<EventABI> = Vec::new();
    let mut errors: Vec<ErrorABI> = Vec::new();
    let mut functions: Vec<FunctionABI> = Vec::new();
    let mut blocks: Vec<Block> = Vec::new();
    let mut transactions: Vec<Transaction> = Vec::new();
    let mut contract_deployments: Vec<ContractDeployment> = Vec::new();
    let mut contract_destructions: Vec<ContractDestruction> = Vec::new();
    let mut logs: Vec<Log> = Vec::new();

    let mut transfers_file_counter = 0;
    let mut events_file_counter = 0;
    let mut errors_file_counter = 0;
    let mut functions_file_counter = 0;
    let mut blocks_file_counter = 0;
    let mut transactions_file_counter = 0;
    let mut contract_deployments_file_counter = 0;
    let mut contract_destructions_file_counter = 0;
    let mut skeletons_file_counter = 0;
    let mut logs_file_counter = 0;

    let mut handles = Vec::new();

    while let Some(comm) = receiver.recv().await {
        match comm {
            WriteCommand::Transfer(transfer) => {
                transfers.push(transfer);
                let size = size_of_val(&*transfers) / 1024; // in KB
                if size > output_size_kb {
                    let o = output_path.to_string();
                    handles.push(tokio::task::spawn_blocking(move || {
                        flush(
                            &transfers,
                            format!(
                                "{}/dynamic/transfers/transfers_{}.json.gz",
                                o, transfers_file_counter
                            )
                            .as_str(),
                            compression_level,
                        );
                    }));
                    transfers_file_counter += 1;
                    transfers = Vec::new();
                }
            }
            WriteCommand::Block(block) => {
                blocks.push(block);
                let size = size_of_val(&*blocks) / 1024; // in kB
                if size > output_size_kb {
                    let o = output_path.to_string();
                    handles.push(tokio::task::spawn_blocking(move || {
                        flush(
                            &blocks,
                            format!("{}/static/blocks/blocks_{}.json.gz", o, blocks_file_counter)
                                .as_str(),
                            compression_level,
                        );
                    }));
                    blocks_file_counter += 1;
                    blocks = Vec::new();
                }
            }
            WriteCommand::Transaction(transaction) => {
                transactions.push(transaction);
                let size = size_of_val(&*transactions) / 1024; // in kB
                if size > output_size_kb {
                    let o = output_path.to_string();
                    handles.push(tokio::task::spawn_blocking(move || {
                        flush(
                            &transactions,
                            format!(
                                "{}/dynamic/transactions/transactions_{}.json.gz",
                                o, transactions_file_counter
                            )
                            .as_str(),
                            compression_level,
                        );
                    }));
                    transactions_file_counter += 1;
                    transactions = Vec::new();
                }
            }
            WriteCommand::ContractDeployment(contract_deployment) => {
                contract_deployments.push(contract_deployment);
                let size = size_of_val(&*contract_deployments) / 1024; // in kB
                if size > output_size_kb {
                    let o = output_path.to_string();
                    handles.push(tokio::task::spawn_blocking(move || {
                        flush(
                            &contract_deployments,
                            format!(
                                "{}/static/deployments/deployments_{}.json.gz",
                                o, contract_deployments_file_counter
                            )
                            .as_str(),
                            compression_level,
                        );
                    }));
                    contract_deployments_file_counter += 1;
                    contract_deployments = Vec::new();
                }
            }
            WriteCommand::Skeleton(skeleton) => {
                skeletons.push(skeleton.clone()); // TODO check this
                if let Some(abi) = skeleton.get_abi() {
                    for node in &abi.nodes {
                        let sig_hash = node.get_signature_hash();
                        match node {
                            ABIStructure::Event(event) => {
                                if stored_event_signatures.contains(&sig_hash) {
                                    continue;
                                }
                                stored_event_signatures.insert(event.get_signature_hash());
                                events.push(event.to_owned());
                            }
                            ABIStructure::Error(error) => {
                                if stored_error_signatures.contains(&sig_hash) {
                                    continue;
                                }
                                stored_error_signatures.insert(error.get_signature_hash());
                                errors.push(error.to_owned());
                            }
                            ABIStructure::Function(function) => {
                                if stored_function_signatures.contains(&sig_hash) {
                                    continue;
                                }
                                stored_function_signatures.insert(function.get_signature_hash());
                                functions.push(function.to_owned());
                            }
                        }
                    }
                }

                let size = size_of_val(&*events) / 1024; // in kB
                if size > output_size_kb {
                    let o = output_path.to_string();
                    handles.push(tokio::task::spawn_blocking(move || {
                        flush(
                            &events,
                            format!("{}/static/events/events_{}.json.gz", o, events_file_counter)
                                .as_str(),
                            compression_level,
                        );
                    }));
                    events_file_counter += 1;
                    events = Vec::new();
                }

                let size = size_of_val(&*errors) / 1024; // in kB
                if size > output_size_kb {
                    let o = output_path.to_string();
                    handles.push(tokio::task::spawn_blocking(move || {
                        flush(
                            &errors,
                            format!("{}/static/errors/errors_{}.json.gz", o, errors_file_counter)
                                .as_str(),
                            compression_level,
                        );
                    }));
                    errors_file_counter += 1;
                    errors = Vec::new();
                }

                let size = size_of_val(&*functions) / 1024; // in kB
                if size > output_size_kb {
                    let o = output_path.to_string();
                    handles.push(tokio::task::spawn_blocking(move || {
                        flush(
                            &functions,
                            format!(
                                "{}/static/functions/functions_{}.json.gz",
                                o, functions_file_counter
                            )
                            .as_str(),
                            compression_level,
                        );
                    }));
                    functions_file_counter += 1;
                    functions = Vec::new();
                }

                let size = size_of_val(&*skeletons) / 1024; // in kB
                if size > output_size_kb {
                    let o = output_path.to_string();
                    handles.push(tokio::task::spawn_blocking(move || {
                        flush(
                            &skeletons,
                            format!(
                                "{}/static/skeletons/skeletons_{}.json.gz",
                                o, skeletons_file_counter
                            )
                            .as_str(),
                            compression_level,
                        );
                    }));
                    skeletons_file_counter += 1;
                    skeletons = Vec::new();
                }
            }
            WriteCommand::ContractDestruction(contract_destruction) => {
                contract_destructions.push(contract_destruction);
                let size = size_of_val(&*contract_destructions) / 1024; // in kB
                if size > output_size_kb {
                    let o = output_path.to_string();
                    handles.push(tokio::task::spawn_blocking(move || {
                        flush(
                            &contract_destructions,
                            format!(
                                "{}/static/destructions/destructions_{}.json.gz",
                                o, contract_destructions_file_counter
                            )
                            .as_str(),
                            compression_level,
                        );
                    }));
                    contract_destructions_file_counter += 1;
                    contract_destructions = Vec::new();
                }
            }
            WriteCommand::Log(log) => {
                logs.push(log);
                let size = size_of_val(&*logs) / 1024; // in kB
                if size > output_size_kb {
                    let o = output_path.to_string();
                    handles.push(tokio::task::spawn_blocking(move || {
                        flush(
                            &logs,
                            format!("{}/dynamic/logs/logs_{}.json.gz", o, logs_file_counter)
                                .as_str(),
                            compression_level,
                        );
                    }));
                    logs_file_counter += 1;
                    logs = Vec::new();
                }
            }
        }
    }

    println!("Flushing remaining data...");

    let now = Instant::now();

    handles.push({
        let o = output_path.to_string();
        tokio::task::spawn_blocking(move || {
            flush(
                &blocks,
                format!("{}/static/blocks/blocks_{}.json.gz", o, blocks_file_counter).as_str(),
                compression_level,
            );
        })
    });

    handles.push({
        let o = output_path.to_string();
        tokio::task::spawn_blocking(move || {
            flush(
                &transactions,
                format!(
                    "{}/dynamic/transactions/transactions_{}.json.gz",
                    o, transactions_file_counter
                )
                .as_str(),
                compression_level,
            );
        })
    });

    handles.push({
        let o = output_path.to_string();
        tokio::task::spawn_blocking(move || {
            flush(
                &contract_deployments,
                format!(
                    "{}/static/deployments/deployments_{}.json.gz",
                    o, contract_deployments_file_counter
                )
                .as_str(),
                compression_level,
            );
        })
    });

    handles.push({
        let o = output_path.to_string();
        tokio::task::spawn_blocking(move || {
            flush(
                &contract_destructions,
                format!(
                    "{}/static/destructions/destructions_{}.json.gz",
                    o, contract_destructions_file_counter
                )
                .as_str(),
                compression_level,
            );
        })
    });

    handles.push({
        let o = output_path.to_string();
        tokio::task::spawn_blocking(move || {
            flush(
                &logs,
                format!("{}/dynamic/logs/logs_{}.json.gz", o, logs_file_counter).as_str(),
                compression_level,
            );
        })
    });

    handles.push({
        let o = output_path.to_string();
        tokio::task::spawn_blocking(move || {
            flush(
                &events,
                format!("{}/static/events/events_{}.json.gz", o, events_file_counter).as_str(),
                compression_level,
            );
        })
    });

    handles.push({
        let o = output_path.to_string();
        tokio::task::spawn_blocking(move || {
            flush(
                &errors,
                format!("{}/static/errors/errors_{}.json.gz", o, errors_file_counter).as_str(),
                compression_level,
            );
        })
    });

    handles.push({
        let o = output_path.to_string();
        tokio::task::spawn_blocking(move || {
            flush(
                &functions,
                format!(
                    "{}/static/functions/functions_{}.json.gz",
                    o, functions_file_counter
                )
                .as_str(),
                compression_level,
            );
        })
    });

    handles.push({
        let o = output_path.to_string();
        tokio::task::spawn_blocking(move || {
            flush(
                &transfers,
                format!(
                    "{}/dynamic/transfers/transfers_{}.json.gz",
                    o, transfers_file_counter
                )
                .as_str(),
                compression_level,
            );
        })
    });

    handles.push({
        let o = output_path.to_string();
        tokio::task::spawn_blocking(move || {
            flush(
                &skeletons,
                format!(
                    "{}/static/skeletons/skeletons_{}.json.gz",
                    o, skeletons_file_counter
                )
                .as_str(),
                compression_level,
            );
        })
    });

    for jh in handles {
        let _ = jh.await;
    }

    let elapsed = now.elapsed();

    println!("Flushing took: {}s", elapsed.as_secs());

    println!("Writer task finished");
}
