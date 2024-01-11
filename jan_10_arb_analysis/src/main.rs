use anchor_lang::solana_program::clock::Slot;
use clap::Parser;
use futures::future::join_all;
use itertools::Itertools;
use lazy_static::lazy_static;
use log::{info, warn};
use solana_sdk::native_token::LAMPORTS_PER_SOL;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::reward_type::RewardType;
use solana_sdk::transaction::VersionedTransaction;
use solana_sdk::vote;
use solana_storage_bigtable::LedgerStorage;
use solana_transaction_status::{ConfirmedBlock, TransactionWithStatusMeta};
use std::collections::{BTreeMap, HashSet};
use std::str::FromStr;
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
    #[arg(short, long, default_value_t = num_cpus::get() * 2)]
    num_cpus: usize,

    /// Token mint to search for
    #[arg(short, long)]
    token_mint: String,
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

    successful_txs_with_mint_cus: u64,
    failed_txs_with_mint_cus: u64,

    success_txs_with_mint: Vec<TransactionWithStatusMeta>,
    failed_txs_with_mint: Vec<TransactionWithStatusMeta>,
}

#[tokio::main]
async fn main() {
    let args: Args = Args::parse();
    env_logger::init();

    let slots: Vec<_> = (args.start_slot..args.end_slot).collect_vec();
    let slot_chunks = slots.chunks(slots.len() / args.num_cpus).collect_vec();
    println!("num slot_chunks: {:?}", slot_chunks.len());

    let (sender, receiver) = channel(10_000);
    let futs = slot_chunks.into_iter().map(|slots| {
        let slots = slots.to_vec();
        let sender = sender.clone();
        tokio::spawn(query_slot_fee_stats(sender, slots))
    });

    let receive_loop = tokio::spawn(examine_token_mint(receiver, args.token_mint));

    join_all(futs).await;

    drop(sender);
    let _ = receive_loop.await;
}

async fn query_slot_fee_stats(sender: Sender<(Slot, ConfirmedBlock)>, slots: Vec<u64>) {
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

fn get_leader(block: &ConfirmedBlock) -> Option<Pubkey> {
    block
        .rewards
        .iter()
        .find(|r| r.reward_type == Some(RewardType::Fee))
        .map(|r| Pubkey::from_str(&r.pubkey))?
        .ok()
}

fn parse_block(slot: &Slot, block: &ConfirmedBlock, token_mint: &String) -> Option<BlockFeeStats> {
    let leader = get_leader(block)?;

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

    let successful_txs_with_mint = filter_txs_with_mint(non_vote_success_txs.clone(), token_mint);
    let successful_txs_with_mint_cus: u64 = successful_txs_with_mint
        .clone()
        .filter_map(|tx| tx.get_status_meta()?.compute_units_consumed)
        .sum();

    let failed_txs_with_mint = filter_txs_with_mint(non_vote_failure_txs.clone(), token_mint);
    let failed_txs_with_mint_cus: u64 = failed_txs_with_mint
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

    let jito_tips_sol = find_jito_tips(&block.transactions);

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
        successful_txs_with_mint_cus,
        failed_txs_with_mint_cus,
        success_txs_with_mint: successful_txs_with_mint.cloned().collect(),
        failed_txs_with_mint: failed_txs_with_mint.cloned().collect(),
    })
}

fn filter_txs_with_mint<'a>(
    transactions: impl Iterator<Item = &'a TransactionWithStatusMeta> + 'a + Clone,
    mint: &'a String,
) -> impl Iterator<Item = &'a TransactionWithStatusMeta> + 'a + Clone {
    transactions.filter_map(|tx| {
        let status_meta = tx.get_status_meta()?;
        if status_meta
            .pre_token_balances?
            .iter()
            .any(|balance| balance.mint == *mint)
            || status_meta
                .post_token_balances?
                .iter()
                .any(|balance| balance.mint == *mint)
        {
            Some(tx)
        } else {
            None
        }
    })
}

fn find_jito_tips(transactions: &[TransactionWithStatusMeta]) -> f64 {
    let mut jito_tips = 0;
    for tx in transactions {
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
    jito_tips as f64 / LAMPORTS_PER_SOL as f64
}

async fn examine_token_mint(mut receiver: Receiver<(Slot, ConfirmedBlock)>, token_mint: String) {
    let mut blocks = BTreeMap::default();

    let mut count = 0;
    while let Some((slot, block)) = receiver.recv().await {
        blocks.insert(slot, block);
        count += 1;
        println!("received {} blocks", count);
    }

    let slots_parsed: Vec<_> = blocks
        .iter()
        .filter_map(|(slot, block)| Some((slot, parse_block(slot, block, &token_mint)?)))
        .collect();

    for (slot, stats) in &slots_parsed {
        info!("slot: {:?}", slot);
        info!("stamp: {:?}", stats.block_time);

        info!("leader: {:?}", stats.leader);

        info!("vote_fees_sol: {:?}", stats.vote_fees_sol);
        info!("non_vote_fees_sol: {:?}", stats.non_vote_fees_sol);
        info!("jito_tips_sol: {:?}", stats.jito_tips_sol);

        info!("non_vote_success_txs: {:?}", stats.non_vote_success_txs);
        info!("non_vote_failure_txs: {:?}", stats.non_vote_failure_txs);

        info!("vote_success_txs: {:?}", stats.vote_success_txs);
        info!("vote_failures_txs: {:?}", stats.vote_failures_txs);

        info!(
            "non_vote_success_compute: {:?}",
            stats.non_vote_success_compute
        );
        info!(
            "non_vote_failure_compute: {:?}",
            stats.non_vote_failure_compute
        );

        info!(
            "successful_txs_with_mint_cus: {:?}",
            stats.successful_txs_with_mint_cus
        );
        info!(
            "failed_txs_with_mint_cus: {:?}",
            stats.failed_txs_with_mint_cus
        );

        info!(
            "success_txs_with_mint: {:?}",
            stats.success_txs_with_mint.len()
        );
        info!(
            "failed_txs_with_mint: {:?}",
            stats.failed_txs_with_mint.len()
        );

        for tx in &stats.success_txs_with_mint {
            info!(
                "success tx. signer: {}, tx: https://solscan.io/tx/{}",
                tx.account_keys()[0],
                tx.transaction_signature()
            );
        }

        for tx in &stats.failed_txs_with_mint {
            info!(
                "failed tx. signer: {}, tx: https://solscan.io/tx/{}",
                tx.account_keys()[0],
                tx.transaction_signature()
            );
        }
        info!("================================");
        info!("");
    }

    let jito_tips: f64 = slots_parsed
        .iter()
        .map(|(slot, stats)| stats.jito_tips_sol)
        .sum();
    let non_vote_fees: f64 = slots_parsed
        .iter()
        .map(|(slot, stats)| stats.non_vote_fees_sol)
        .sum();

    let total_token_cus: u64 = slots_parsed
        .iter()
        .map(|(slot, stats)| stats.failed_txs_with_mint_cus + stats.successful_txs_with_mint_cus)
        .sum();

    let total_token_failed_cus: u64 = slots_parsed
        .iter()
        .map(|(slot, stats)| stats.failed_txs_with_mint_cus)
        .sum();

    let total_non_vote_cus: u64 = slots_parsed
        .iter()
        .map(|(slot, stats)| stats.non_vote_failure_compute + stats.non_vote_success_compute)
        .sum();

    info!("total jito tips: {:?}", jito_tips);
    info!("non vote fees: {:?}", non_vote_fees);
    info!("total_token_cus: {:?}", total_token_cus);
    info!("total_token_failed_cus: {:?}", total_token_failed_cus);
    info!("total_non_vote_cus: {:?}", total_non_vote_cus);
}
