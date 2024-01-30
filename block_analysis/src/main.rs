use clap::Parser;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use solana_storage_bigtable::LedgerStorage;
use solana_transaction_status::TransactionWithStatusMeta;
use std::collections::{HashMap, HashSet};

#[derive(Parser)]
struct Args {
    /// RPC URL
    #[arg(short, long)]
    rpc_url: String,

    /// Leaders to examine slots for
    #[arg(short, long)]
    leaders: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Args { rpc_url, leaders } = Args::parse();

    let leaders: HashSet<_> = leaders.iter().collect();

    let rpc_client = RpcClient::new(rpc_url);

    let epoch = rpc_client.get_epoch_info().await?;

    let slot_offset = epoch.absolute_slot - epoch.slot_index;

    let leader_schedule = rpc_client
        .get_leader_schedule(Some(epoch.absolute_slot))
        .await?
        .unwrap();

    let leader_slots_of_interest: HashMap<String, Vec<u64>> = leader_schedule
        .into_iter()
        .filter(|(leader, _)| leaders.contains(leader))
        .map(|(leader, slots)| {
            (
                leader,
                slots.iter().map(|s| (*s as u64 + slot_offset)).collect(),
            )
        })
        .collect();

    let slots: Vec<_> = leader_slots_of_interest
        .values()
        .flat_map(|s| s)
        .filter(|s| **s <= epoch.absolute_slot)
        .cloned()
        .collect();

    let ledger_storage = LedgerStorage::new(true, None, None).await?;

    println!("reading {} slots from bigtable", slots.len());
    let slots_blocks = ledger_storage
        .get_confirmed_blocks_with_data(&slots)
        .await?;

    for (slot, block) in slots_blocks {
        let writable_keys_sorted = find_writable_keys_sorted(&block.transactions);

        // take the top 5 write accounts per block and find the transaction details
        for (account_idx, (account, _)) in writable_keys_sorted.iter().enumerate().take(5) {
            let transactions_write_locking_account =
                find_transactions_by_write_lock(&block.transactions, account);
            println!(
                "slot: {} used_in_slot: {} account: {} transactions: {}",
                slot,
                account_idx,
                account,
                transactions_write_locking_account.len()
            );

            let mut compute_used_so_far = 0;

            println!(
                "{:<3} | {:<6} | {:<6} | {:<6} | {:<7} | {:<7} | {:<8}",
                "idx", "fee", "sig", "signer", "success", "compute", "cus_so_far"
            );
            for (idx, tx) in transactions_write_locking_account {
                let meta = tx.get_status_meta().unwrap();

                println!(
                    "{:<3} | {:<6} | {:<6} | {:<6} | {:<7} | {:<7} | {:<8}",
                    idx,
                    meta.fee,
                    &tx.transaction_signature().to_string()[..5],
                    &tx.account_keys()[0].to_string()[..5],
                    meta.status.is_ok(),
                    meta.compute_units_consumed.unwrap_or_default(),
                    compute_used_so_far,
                );

                compute_used_so_far += meta.compute_units_consumed.unwrap_or_default();
            }

            println!();
        }
        println!("------");
    }

    Ok(())
}

fn find_transactions_by_write_lock<'a>(
    transactions: &'a [TransactionWithStatusMeta],
    account: &Pubkey,
) -> Vec<(usize, &'a TransactionWithStatusMeta)> {
    transactions
        .iter()
        .enumerate()
        .filter_map(|(idx, tx)| {
            let versioned_tx = tx.get_transaction();
            if tx
                .account_keys()
                .iter()
                .enumerate()
                .any(|(account_idx, a)| {
                    a == account && versioned_tx.message.is_maybe_writable(account_idx)
                })
            {
                Some((idx, tx))
            } else {
                None
            }
        })
        .collect()
}

/// Finds writable accounts, sorted by number of accesses
fn find_writable_keys_sorted(txs: &[TransactionWithStatusMeta]) -> Vec<(Pubkey, usize)> {
    let writable_keys = txs.iter().fold(HashMap::new(), |mut hmap, tx| {
        let versioned_tx = tx.get_transaction();

        tx.account_keys()
            .iter()
            .enumerate()
            .for_each(|(index, acc)| {
                if versioned_tx.message.is_maybe_writable(index) {
                    hmap.entry(*acc)
                        .and_modify(|e| *e += 1_usize)
                        .or_insert(1_usize);
                }
            });
        hmap
    });

    let mut write_locks_per_account: Vec<_> = writable_keys.into_iter().collect();
    write_locks_per_account.sort_by(|a, b| b.1.cmp(&a.1));
    write_locks_per_account
}
