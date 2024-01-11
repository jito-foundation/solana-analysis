use anchor_lang::solana_program::clock::Slot;
use clap::Parser;
use csv::Writer;
use futures::future::join_all;
use itertools::Itertools;
use lazy_static::lazy_static;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use solana_sdk::native_token::LAMPORTS_PER_SOL;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::reward_type::RewardType;
use solana_sdk::transaction::VersionedTransaction;
use solana_sdk::vote;
use solana_storage_bigtable::LedgerStorage;
use solana_transaction_status::{ConfirmedBlock, TransactionWithStatusMeta};
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{channel, Receiver, Sender};

lazy_static! {
    static ref TIP_ACCOUNTS: HashSet<Pubkey> = HashSet::from_iter([
        Pubkey::from_str("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5").unwrap(),
        Pubkey::from_str("HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe").unwrap(),
        Pubkey::from_str("Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY").unwrap(),
        Pubkey::from_str("ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49").unwrap(),
        Pubkey::from_str("DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh").unwrap(),
        Pubkey::from_str("ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt").unwrap(),
        Pubkey::from_str("DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL").unwrap(),
        Pubkey::from_str("3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT").unwrap(),
    ]);
}

#[derive(Parser)]
struct Args {
    /// Start slot
    #[arg(short, long)]
    start_slot: u64,

    /// End slot
    #[arg(short, long)]
    end_slot: u64,

    /// End slot
    #[arg(short, long, default_value_t = num_cpus::get() * 2)]
    num_cpus: usize,

    /// Token mint to search for
    #[arg(short, long)]
    token_mint: String,
}

#[tokio::main]
async fn main() {
    let args: Args = Args::parse();
    env_logger::init();

    let slots: Vec<_> = (args.start_slot..args.end_slot).collect_vec();
    let slot_chunks = slots.chunks(slots.len() / args.num_cpus).collect_vec();
    info!(
        "querying {} slots using {} slot_chunks",
        slots.len(),
        slot_chunks.len()
    );

    let mint_tx_csv = csv::WriterBuilder::new()
        .from_path(format!(
            "{}_{}_{}_{}_stats.csv",
            args.start_slot,
            args.end_slot,
            args.token_mint,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        ))
        .unwrap();

    let block_stats_csv = csv::WriterBuilder::new()
        .from_path(format!(
            "{}_{}_{}_block_stats.csv",
            args.start_slot,
            args.end_slot,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        ))
        .unwrap();

    let (sender, receiver) = channel(10_000);
    let futs = slot_chunks.into_iter().map(|slots| {
        let slots = slots.to_vec();
        let sender = sender.clone();
        tokio::spawn(read_slots(sender, slots))
    });

    let receive_loop = tokio::spawn(gather_and_emit_stats(
        receiver,
        args.token_mint,
        mint_tx_csv,
        block_stats_csv,
    ));

    join_all(futs).await;

    drop(sender);
    let _ = receive_loop.await;
}

async fn read_slots(sender: Sender<(Slot, ConfirmedBlock)>, slots: Vec<u64>) {
    let ledger_tool = loop {
        match LedgerStorage::new(true, None, None).await {
            Ok(l) => {
                break l;
            }
            Err(e) => {
                println!("error connecting: {:?}", e);
            }
        }
    };

    for slots_chunk in slots.chunks(10) {
        match ledger_tool
            .get_confirmed_blocks_with_data(slots_chunk)
            .await
        {
            Ok(slots_blocks) => {
                for (slot, block) in slots_blocks {
                    let _ = sender.send((slot, block)).await;
                }
            }
            Err(e) => {
                warn!("error reading slots: {:?}", e);
            }
        }
    }
}

fn is_simple_vote_transaction(tx: &VersionedTransaction) -> bool {
    if tx.signatures.len() < 3 && tx.message.instructions().len() == 1
    // && matches!(tx.message, SanitizedMessage::Legacy(_))
    {
        let mut ix_iter = tx.message.instructions().iter();
        ix_iter
            .next()
            .map(|ix| tx.message.static_account_keys()[ix.program_id_index as usize])
            == Some(vote::program::id())
    } else {
        false
    }
}

fn get_leader(block: &ConfirmedBlock) -> Option<Pubkey> {
    block
        .rewards
        .iter()
        .find(|r| r.reward_type == Some(RewardType::Fee))
        .map(|r| Pubkey::from_str(&r.pubkey))?
        .ok()
}

fn calculate_jito_tips(tx: &TransactionWithStatusMeta) -> u64 {
    let pre_tx_balances = tx.get_status_meta().unwrap().pre_balances;
    let post_tx_balances = tx.get_status_meta().unwrap().post_balances;

    let mut jito_tips = 0;
    for (idx, account) in tx.account_keys().iter().enumerate() {
        if TIP_ACCOUNTS.contains(account) {
            if post_tx_balances[idx] > pre_tx_balances[idx] {
                jito_tips += post_tx_balances[idx] - pre_tx_balances[idx];
            }
        }
    }
    jito_tips
}

#[derive(Default, Serialize, Deserialize)]
struct TransactionStats {
    slot: u64,
    block_time: i64,
    index: u64,
    leader: String,
    signature: String,
    signer: String,
    is_success: bool,
    compute_units: u64,
    fees_paid: f64,
    jito_tips: f64,
    link: String,
}

#[derive(Default, Serialize, Deserialize)]
struct BlockStats {
    slot: u64,
    block_time: i64,
    leader: String,
    fees_paid: f64,
    jito_tips: f64,

    total_ok_cus: u64,
    total_fail_cus: u64,
    total_cus: u64,

    total_non_vote_ok_cus: u64,
    total_non_vote_fail_cus: u64,
    total_non_vote_cus: u64,

    non_vote_txs: usize,
    vote_txs: usize,
    total_txs: usize,
}

async fn gather_and_emit_stats(
    mut receiver: Receiver<(Slot, ConfirmedBlock)>,
    token_mint: String,
    mut mint_tx_csv: Writer<File>,
    mut block_stats_csv: Writer<File>,
) {
    let mut blocks = BTreeMap::default();

    let mut count = 0;
    while let Some((slot, block)) = receiver.recv().await {
        blocks.insert(slot, block);
        count += 1;
        println!("received {} blocks", count);
    }

    for (slot, block) in &blocks {
        let leader = get_leader(block).unwrap().to_string();

        let transactions_touching_token_mint: Vec<_> = block
            .transactions
            .iter()
            .enumerate()
            .filter_map(|(idx, tx)| {
                let status_meta = tx.get_status_meta()?;
                if status_meta
                    .pre_token_balances?
                    .iter()
                    .any(|balance| balance.mint == *token_mint)
                    || status_meta
                        .post_token_balances?
                        .iter()
                        .any(|balance| balance.mint == *token_mint)
                {
                    Some((idx, tx))
                } else {
                    None
                }
            })
            .collect();

        let transaction_stats: Vec<_> = transactions_touching_token_mint
            .iter()
            .map(|(idx, tx)| {
                let status_meta = tx.get_status_meta().unwrap();

                TransactionStats {
                    slot: *slot,
                    block_time: block.block_time.unwrap(),
                    index: *idx as u64,
                    leader: leader.clone(),
                    signature: tx.transaction_signature().to_string(),
                    signer: tx.account_keys()[0].to_string(),
                    is_success: status_meta.status.is_ok(),
                    compute_units: status_meta.compute_units_consumed.unwrap(),
                    fees_paid: status_meta.fee as f64 / LAMPORTS_PER_SOL as f64,
                    jito_tips: calculate_jito_tips(tx) as f64 / LAMPORTS_PER_SOL as f64,
                    link: format!("https://solscan.io/tx/{}", tx.transaction_signature()),
                }
            })
            .collect();

        for stat in transaction_stats {
            mint_tx_csv.serialize(stat).unwrap();
        }

        let jito_tips = block
            .transactions
            .iter()
            .map(calculate_jito_tips)
            .sum::<u64>() as f64
            / LAMPORTS_PER_SOL as f64;
        let total_ok_cus: u64 = block
            .transactions
            .iter()
            .filter(|tx| tx.get_status_meta().unwrap().status.is_ok())
            .map(|tx| {
                tx.get_status_meta()
                    .unwrap()
                    .compute_units_consumed
                    .unwrap()
            })
            .sum();
        let total_fail_cus: u64 = block
            .transactions
            .iter()
            .filter(|tx| tx.get_status_meta().unwrap().status.is_err())
            .map(|tx| {
                tx.get_status_meta()
                    .unwrap()
                    .compute_units_consumed
                    .unwrap()
            })
            .sum();

        let total_non_vote_ok_cus: u64 = block
            .transactions
            .iter()
            .filter(|tx| !is_simple_vote_transaction(&tx.get_transaction()))
            .filter(|tx| tx.get_status_meta().unwrap().status.is_ok())
            .map(|tx| {
                tx.get_status_meta()
                    .unwrap()
                    .compute_units_consumed
                    .unwrap()
            })
            .sum();
        let total_non_vote_fail_cus: u64 = block
            .transactions
            .iter()
            .filter(|tx| !is_simple_vote_transaction(&tx.get_transaction()))
            .filter(|tx| tx.get_status_meta().unwrap().status.is_err())
            .map(|tx| {
                tx.get_status_meta()
                    .unwrap()
                    .compute_units_consumed
                    .unwrap()
            })
            .sum();

        let fees_paid = block
            .transactions
            .iter()
            .map(|tx| tx.get_status_meta().unwrap().fee)
            .sum::<u64>() as f64
            / LAMPORTS_PER_SOL as f64;

        let non_vote_txs = block
            .transactions
            .iter()
            .filter(|tx| !is_simple_vote_transaction(&tx.get_transaction()))
            .count();
        let vote_txs = block
            .transactions
            .iter()
            .filter(|tx| is_simple_vote_transaction(&tx.get_transaction()))
            .count();

        block_stats_csv
            .serialize(BlockStats {
                slot: *slot,
                block_time: block.block_time.unwrap(),
                leader,
                fees_paid,
                jito_tips,
                total_ok_cus,
                total_fail_cus,
                total_cus: total_ok_cus + total_fail_cus,
                total_non_vote_ok_cus,
                total_non_vote_fail_cus,
                total_non_vote_cus: total_non_vote_ok_cus + total_non_vote_fail_cus,
                non_vote_txs,
                vote_txs,
                total_txs: non_vote_txs + vote_txs,
            })
            .unwrap();
    }

    block_stats_csv.flush().unwrap();
    mint_tx_csv.flush().unwrap();
}
