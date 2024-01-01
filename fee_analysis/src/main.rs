use anchor_lang::solana_program::clock::Slot;
use clap::Parser;
use csv::Writer;
use futures::future::join_all;
use itertools::Itertools;
use lazy_static::lazy_static;
use log::warn;
use serde::{Deserialize, Serialize};
use solana_sdk::native_token::LAMPORTS_PER_SOL;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::reward_type::RewardType;
use solana_sdk::transaction::VersionedTransaction;
use solana_sdk::vote;
use solana_storage_bigtable::LedgerStorage;
use solana_transaction_status::ConfirmedBlock;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[derive(Parser)]
struct Args {
    /// Start slot
    #[arg(short, long)]
    start_slot: u64,

    /// End slot
    #[arg(short, long)]
    end_slot: u64,

    /// End slot
    #[arg(short, long, default_value_t = num_cpus::get() * 3)]
    num_cpus: usize,
}

#[derive(Debug, Clone)]
struct BlockFeeStats {
    leader: Pubkey,
    block_time: i64,
    slot: u64,
    vote_fees_sol: f64,
    non_vote_fees_sol: f64,
    jito_tips_sol: f64,

    non_vote_success_txs: usize,
    non_vote_failure_txs: usize,
    vote_success_txs: usize,
    vote_failures_txs: usize,

    non_vote_success_compute: u64,
    non_vote_failure_compute: u64,
}

#[tokio::main]
async fn main() {
    let args: Args = Args::parse();

    let slots: Vec<_> = (args.start_slot..args.end_slot).collect_vec();
    let slot_chunks = slots.chunks(slots.len() / args.num_cpus).collect_vec();
    println!("num slot_chunks: {:?}", slot_chunks.len());

    let (sender, receiver) = channel(10_000);
    let futs = slot_chunks.into_iter().map(|slots| {
        let slots = slots.to_vec();
        let sender = sender.clone();
        tokio::spawn(query_slot_fee_stats(sender, slots))
    });

    let csv = csv::WriterBuilder::new()
        .from_path(format!(
            "{}_{}_{}_stats.csv",
            args.start_slot,
            args.end_slot,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        ))
        .unwrap();
    let receive_loop = tokio::spawn(aggregate_slot_fee_stats(receiver, csv));

    join_all(futs).await;

    drop(sender);
    let _ = receive_loop.await;
}

async fn query_slot_fee_stats(sender: Sender<BlockFeeStats>, slots: Vec<u64>) {
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
                    if let Some(fee_stats) = parse_block_fees(&slot, &block) {
                        let _ = sender.send(fee_stats).await;
                    }
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

fn parse_block_fees(slot: &Slot, block: &ConfirmedBlock) -> Option<BlockFeeStats> {
    let leader = block
        .rewards
        .iter()
        .find(|r| r.reward_type == Some(RewardType::Fee))
        .map(|r| Pubkey::from_str(&r.pubkey))?
        .ok()?;

    // Non-vote transactions

    let non_vote_txs = block
        .transactions
        .iter()
        .filter(|tx| !is_simple_vote_transaction(&tx.get_transaction()));
    let non_vote_success_txs = non_vote_txs
        .clone()
        .filter(|tx| tx.get_status_meta().unwrap().status.is_ok());
    let non_vote_failure_txs = non_vote_txs
        .clone()
        .filter(|tx| tx.get_status_meta().unwrap().status.is_err());

    let non_vote_fees: u64 = non_vote_txs
        .clone()
        .filter_map(|tx| {
            let status = tx.get_status_meta()?;
            Some(status.fee)
        })
        .sum();
    let non_vote_fees_sol = non_vote_fees as f64 / LAMPORTS_PER_SOL as f64;

    let non_vote_success_compute_units = non_vote_success_txs
        .clone()
        .filter_map(|tx| tx.get_status_meta()?.compute_units_consumed)
        .sum();

    let non_vote_failed_compute_units = non_vote_failure_txs
        .clone()
        .filter_map(|tx| tx.get_status_meta()?.compute_units_consumed)
        .sum();

    let non_vote_success_tx_count = non_vote_success_txs.count();
    let non_vote_failure_tx_count = non_vote_failure_txs.count();

    // Vote transactions

    let vote_txs = block
        .transactions
        .iter()
        .filter(|tx| is_simple_vote_transaction(&tx.get_transaction()));
    let vote_success_txs = vote_txs
        .clone()
        .filter(|tx| tx.get_status_meta().unwrap().status.is_ok());
    let vote_failure_txs = vote_txs
        .clone()
        .filter(|tx| tx.get_status_meta().unwrap().status.is_err());

    let vote_fees: u64 = vote_txs
        .clone()
        .filter_map(|tx| {
            let status = tx.get_status_meta()?;
            Some(status.fee)
        })
        .sum();
    let vote_fees_sol = vote_fees as f64 / LAMPORTS_PER_SOL as f64;

    let vote_success_tx_count = vote_success_txs.count();
    let vote_failure_tx_count = vote_failure_txs.count();

    let mut jito_tips = 0;
    for tx in &block.transactions {
        let pre_tx_balances = tx.get_status_meta().unwrap().pre_balances;
        let post_tx_balances = tx.get_status_meta().unwrap().post_balances;
        for (idx, account) in tx.account_keys().iter().enumerate() {
            if TIP_ACCOUNTS.contains(account) {
                if post_tx_balances[idx] > pre_tx_balances[idx] {
                    jito_tips += post_tx_balances[idx] - pre_tx_balances[idx];
                }
            }
        }
    }
    let jito_tips_sol = jito_tips as f64 / LAMPORTS_PER_SOL as f64;

    // divide by 2 for burned fees
    Some(BlockFeeStats {
        leader,
        block_time: block.block_time.unwrap(),
        slot: *slot,
        non_vote_fees_sol: non_vote_fees_sol / 2.0,
        vote_fees_sol: vote_fees_sol / 2.0,
        jito_tips_sol,
        non_vote_success_txs: non_vote_success_tx_count,
        non_vote_failure_txs: non_vote_failure_tx_count,
        vote_success_txs: vote_success_tx_count,
        vote_failures_txs: vote_failure_tx_count,
        non_vote_success_compute: non_vote_success_compute_units,
        non_vote_failure_compute: non_vote_failed_compute_units,
    })
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct AggregatedStats {
    leader: String,
    num_blocks: u64,
    non_vote_fees_sol: f64,
    vote_fees_sol: f64,
    jito_tips_sol: f64,

    non_vote_success_txs: usize,
    non_vote_failure_txs: usize,
    non_vote_success_compute: u64,
    non_vote_failure_compute: u64,
}

async fn aggregate_slot_fee_stats(mut receiver: Receiver<BlockFeeStats>, mut csv: Writer<File>) {
    let mut aggregated_stats: HashMap<Pubkey, AggregatedStats> = HashMap::default();

    let mut count = 0;

    while let Some(stats) = receiver.recv().await {
        aggregated_stats
            .entry(stats.leader)
            .and_modify(|s| {
                s.num_blocks += 1;
                s.non_vote_fees_sol += stats.non_vote_fees_sol;
                s.vote_fees_sol += stats.vote_fees_sol;
                s.jito_tips_sol += stats.jito_tips_sol;
                s.non_vote_success_txs += stats.non_vote_success_txs;
                s.non_vote_failure_txs += stats.non_vote_failure_txs;
                s.non_vote_success_compute += stats.non_vote_success_compute;
                s.non_vote_failure_compute += stats.non_vote_failure_compute;
            })
            .or_insert(AggregatedStats {
                leader: stats.leader.to_string(),
                num_blocks: 1,
                non_vote_fees_sol: stats.non_vote_fees_sol,
                vote_fees_sol: stats.vote_fees_sol,
                jito_tips_sol: stats.jito_tips_sol,
                non_vote_success_txs: stats.non_vote_success_txs,
                non_vote_failure_txs: stats.non_vote_failure_txs,
                non_vote_success_compute: stats.non_vote_success_compute,
                non_vote_failure_compute: stats.non_vote_failure_compute,
            });

        count += 1;
        println!("received {} blocks", count);
    }

    // poor mans csv
    for (_, stats) in aggregated_stats {
        csv.serialize(stats).unwrap();
    }
    csv.flush().unwrap();
}
