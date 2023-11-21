// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{errors::*, task::ExecutorTask};
use aptos_aggregator::types::PanicOr;
use aptos_logger::error;
use aptos_mvhashmap::types::ValueWithLayout;
use aptos_types::{
    transaction::BlockExecutableTransaction as Transaction, write_set::TransactionWrite,
};
use bytes::Bytes;
use std::{collections::BTreeMap, sync::Arc};

macro_rules! groups_to_finalize {
    ($outputs:expr, $($txn_idx:expr),*) => {{
	let group_write_ops = $outputs.resource_group_metadata_ops($($txn_idx),*);

        group_write_ops.into_iter()
            .map(|val| (val, false))
            .chain([()].into_iter().flat_map(|_| {
		// Lazily evaluated only after iterating over group_write_ops.
                $outputs.group_reads_needing_delayed_field_exchange($($txn_idx),*)
                    .into_iter()
                    .map(|val| (val, true))
            }))
    }};
}

pub(crate) use groups_to_finalize;

pub(crate) fn resource_group_error(err_msg: String) -> PanicOr<IntentionalFallbackToSequential> {
    error!("resource_group_error: {:?}", err_msg);
    PanicOr::Or(IntentionalFallbackToSequential::ResourceGroupError(err_msg))
}

pub(crate) fn map_finalized_group<T: Transaction, E: ExecutorTask<Txn = T>>(
    group_key: T::Key,
    finalized_group: anyhow::Result<Vec<(T::Tag, ValueWithLayout<T::Value>)>>,
    metadata_op: T::Value,
    is_read_needing_exchange: bool,
) -> Result<(T::Key, T::Value, Vec<(T::Tag, ValueWithLayout<T::Value>)>), E::Error> {
    let metadata_is_deletion = metadata_op.is_deletion();

    match finalized_group {
        Ok(finalized_group) => {
            if is_read_needing_exchange && metadata_is_deletion {
                // Value needed exchange but was not written / modified during the txn
                // execution: may not be empty.
                Err(Error::FallbackToSequential(resource_group_error(format!(
                    "Value only read and exchanged, but metadata op is Deletion",
                ))))
            } else if finalized_group.is_empty() != metadata_is_deletion {
                // finalize_group already applies the deletions.
                Err(Error::FallbackToSequential(resource_group_error(format!(
                    "Group is empty = {} but op is deletion = {} in parallel execution",
                    finalized_group.is_empty(),
                    metadata_is_deletion
                ))))
            } else {
                Ok((group_key, metadata_op, finalized_group))
            }
        },
        Err(e) => Err(Error::FallbackToSequential(resource_group_error(format!(
            "Error committing resource group {:?}",
            e
        )))),
    }
}

pub(crate) fn serialize_groups<T: Transaction>(
    finalized_groups: Vec<(T::Key, T::Value, Vec<(T::Tag, Arc<T::Value>)>)>,
) -> ::std::result::Result<Vec<(T::Key, T::Value)>, PanicOr<IntentionalFallbackToSequential>> {
    finalized_groups
        .into_iter()
        .map(|(group_key, mut metadata_op, finalized_group)| {
            let btree: BTreeMap<T::Tag, Bytes> = finalized_group
                .into_iter()
                // TODO[agg_v2](fix): Should anything be done using the layout here?
                .map(|(resource_tag, arc_v)| {
                    let bytes = arc_v
                        .extract_raw_bytes()
                        .expect("Deletions should already be applied");
                    (resource_tag, bytes)
                })
                .collect();

            // TODO[agg_v2](fix): Handle potential serialization failures.
            bcs::to_bytes(&btree)
                .map_err(|e| {
                    resource_group_error(format!("Unexpected resource group error {:?}", e))
                })
                .map(|group_bytes| {
                    metadata_op.set_bytes(group_bytes.into());
                    (group_key, metadata_op)
                })
        })
        .collect()
}
