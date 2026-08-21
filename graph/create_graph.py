from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import torch
from sklearn.model_selection import train_test_split
from torch_geometric.data import HeteroData
from preprocessing.paths import PROCESSED_DATA_DIR

ACCOUNT_FEATURE_COLUMNS = [
    "outgoing_txn_count",
    "incoming_txn_count",
    "outgoing_unique_counterparties",
    "incoming_unique_counterparties",
    "repeat_counterparty_ratio",
    "self_transfer_ratio",
    "outgoing_total_paid",
    "incoming_total_received",
    "outgoing_avg_paid",
    "incoming_avg_received",
    "outgoing_amount_std",
    "incoming_amount_std",
    "payment_currency_diversity",
    "receiving_currency_diversity",
    "currency_switch_ratio",
    "payment_format_diversity",
    "unique_connected_banks",
    "cross_bank_txn_ratio",
    "odd_hour_txn_ratio",
    "weekend_txn_ratio",
    "active_span_hours",
    "hours_since_last_txn",
    "max_txns_per_hour",
    "avg_inter_txn_gap_hours",
]

BANK_FEATURE_COLUMNS = [
    "bank_account_count",
    "outgoing_txn_count",
    "incoming_txn_count",
    "outgoing_total_paid",
    "incoming_total_received",
    "outgoing_avg_paid",
    "incoming_avg_received",
    "unique_sender_accounts",
    "unique_receiver_accounts",
    "counterparty_bank_count",
    "currency_diversity",
    "payment_format_diversity",
    "cross_bank_txn_ratio",
    "odd_hour_txn_ratio",
    "weekend_txn_ratio",
]

ENTITY_FEATURE_COLUMNS = [
    "entity_type_sole_proprietorship",
    "entity_type_partnership",
    "entity_type_corporation",
    "entity_type_country",
    "entity_type_other",
    "owned_account_count",
    "connected_bank_count",
    "outgoing_txn_count",
    "incoming_txn_count",
    "outgoing_total_paid",
    "incoming_total_received",
    "unique_counterparty_accounts",
    "cross_bank_txn_ratio",
    "odd_hour_txn_ratio",
    "payment_format_diversity",
]

ACCOUNT_TO_ACCOUNT_EDGE_COLUMNS = [
    "edge_txn_count",
    "edge_total_paid",
    "edge_avg_paid",
    "edge_amount_std",
    "edge_is_repeated",
    "edge_recency_hours",
    "edge_currency_switch_ratio",
    "edge_payment_format_diversity",
    "edge_inter_txn_gap_hours",
]


def require_columns(
    dataframe: pd.DataFrame,
    required_columns: list[str],
    file_name: str,
) -> None:
    missing = sorted(set(required_columns) - set(dataframe.columns))

    if missing:
        raise ValueError(
            f"{file_name} is missing required columns: {missing}"
        )


def load_input_tables(
    input_dir: Path,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    """
    Load the final feature-engineering outputs.
    """
    account_features = pd.read_parquet(
        input_dir / "node_account_features.parquet"
    )
    bank_features = pd.read_parquet(
        input_dir / "node_bank_features.parquet"
    )
    entity_features = pd.read_parquet(
        input_dir / "node_entity_features.parquet"
    )
    account_to_account_edges = pd.read_parquet(
        input_dir / "edge_account_to_account.parquet"
    )
    account_to_bank_edges = pd.read_parquet(
        input_dir / "edge_account_to_bank.parquet"
    )
    entity_to_account_edges = pd.read_parquet(
        input_dir / "edge_entity_to_account.parquet"
    )
    account_labels = pd.read_parquet(
        input_dir / "account_labels.parquet"
    )

    require_columns(
        account_features,
        ["account_id"] + ACCOUNT_FEATURE_COLUMNS,
        "node_account_features.parquet",
    )
    require_columns(
        bank_features,
        ["bank_id"] + BANK_FEATURE_COLUMNS,
        "node_bank_features.parquet",
    )
    require_columns(
        entity_features,
        ["entity_id"] + ENTITY_FEATURE_COLUMNS,
        "node_entity_features.parquet",
    )
    require_columns(
        account_to_account_edges,
        [
            "sender_account_id",
            "receiver_account_id",
        ] + ACCOUNT_TO_ACCOUNT_EDGE_COLUMNS,
        "edge_account_to_account.parquet",
    )
    require_columns(
        account_to_bank_edges,
        ["account_id", "bank_id"],
        "edge_account_to_bank.parquet",
    )
    require_columns(
        entity_to_account_edges,
        ["entity_id", "account_id"],
        "edge_entity_to_account.parquet",
    )
    require_columns(
        account_labels,
        ["account_id", "account_label"],
        "account_labels.parquet",
    )

    return (
        account_features,
        bank_features,
        entity_features,
        account_to_account_edges,
        account_to_bank_edges,
        entity_to_account_edges,
        account_labels,
    )


def build_index_map(
    dataframe: pd.DataFrame,
    id_column: str,
) -> tuple[list[str], dict[str, int]]:
    """
    Convert original string IDs into the consecutive integer indexes PyG uses.
    """
    ids = dataframe[id_column].astype(str).tolist()

    if len(ids) != len(set(ids)):
        raise ValueError(f"{id_column} contains duplicate node IDs.")

    return ids, {node_id: index for index, node_id in enumerate(ids)}


def dataframe_to_tensor(
    dataframe: pd.DataFrame,
    feature_columns: list[str],
) -> torch.Tensor:
    values = dataframe[feature_columns].to_numpy(dtype=np.float32)

    if not np.isfinite(values).all():
        raise ValueError("Node feature matrix contains NaN or infinite values.")

    return torch.tensor(values, dtype=torch.float32)


def make_edge_index(
    dataframe: pd.DataFrame,
    source_column: str,
    destination_column: str,
    source_map: dict[str, int],
    destination_map: dict[str, int],
) -> torch.Tensor:
    source_ids = dataframe[source_column].astype(str)
    destination_ids = dataframe[destination_column].astype(str)

    source_indexes = source_ids.map(source_map)
    destination_indexes = destination_ids.map(destination_map)

    missing_source_count = int(source_indexes.isna().sum())
    missing_destination_count = int(destination_indexes.isna().sum())

    if missing_source_count or missing_destination_count:
        raise ValueError(
            "An edge references a node missing from the node tables. "
            f"Missing source nodes: {missing_source_count}; "
            f"missing destination nodes: {missing_destination_count}."
        )

    source_indexes_np = source_indexes.to_numpy(dtype=np.int64)
    destination_indexes_np = destination_indexes.to_numpy(dtype=np.int64)

    return torch.tensor(
        np.vstack([source_indexes_np, destination_indexes_np]),
        dtype=torch.long,
    )


def build_masks(
    labels: torch.Tensor,
    test_mode: bool,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """
    Build stratified Account train/validation/test masks.

    The 100,000-row test dataset has only two positive labels, so it can only
    create a graph in --test-mode. The full Medium dataset will use a proper
    70/15/15 stratified split.
    """
    labels_np = labels.numpy()
    positive_count = int((labels_np == 1).sum())
    negative_count = int((labels_np == 0).sum())

    total_nodes = len(labels_np)

    train_mask = torch.zeros(total_nodes, dtype=torch.bool)
    validation_mask = torch.zeros(total_nodes, dtype=torch.bool)
    test_mask = torch.zeros(total_nodes, dtype=torch.bool)

    if min(positive_count, negative_count) < 3:
        if not test_mode:
            raise ValueError(
                "Not enough positive/negative labels for a stratified split. "
                "Use --test-mode only for the 100,000-row smoke test."
            )

        train_mask[:] = True
        print(
            "Warning: test-mode uses every Account node as training data. "
            "No model should be evaluated from this small test graph."
        )
        return train_mask, validation_mask, test_mask

    all_indexes = np.arange(total_nodes)

    train_indexes, temporary_indexes = train_test_split(
        all_indexes,
        test_size=0.30,
        stratify=labels_np,
        random_state=42,
    )

    validation_indexes, test_indexes = train_test_split(
        temporary_indexes,
        test_size=0.50,
        stratify=labels_np[temporary_indexes],
        random_state=42,
    )

    train_mask[train_indexes] = True
    validation_mask[validation_indexes] = True
    test_mask[test_indexes] = True

    return train_mask, validation_mask, test_mask


def build_graph(
    test_mode: bool,
    input_dir: Path,
) -> tuple[
    HeteroData,
    dict[str, list[str]],
]:
    (
        account_features,
        bank_features,
        entity_features,
        account_to_account_edges,
        account_to_bank_edges,
        entity_to_account_edges,
        account_labels,
    ) = load_input_tables(input_dir)

    # Labels are separate from the Account feature table by design.
    account_features = account_features.merge(
        account_labels,
        on="account_id",
        how="left",
        validate="one_to_one",
    )

    if account_features["account_label"].isna().any():
        raise ValueError("Some Account nodes do not have an offline label.")

    account_ids, account_map = build_index_map(
        account_features,
        "account_id",
    )
    bank_ids, bank_map = build_index_map(bank_features, "bank_id")
    entity_ids, entity_map = build_index_map(
        entity_features,
        "entity_id",
    )

    data = HeteroData()

    # Node feature matrices and labels
    data["account"].x = dataframe_to_tensor(
        account_features,
        ACCOUNT_FEATURE_COLUMNS,
    )
    data["account"].y = torch.tensor(
        account_features["account_label"].to_numpy(dtype=np.int64),
        dtype=torch.long,
    )

    data["bank"].x = dataframe_to_tensor(
        bank_features,
        BANK_FEATURE_COLUMNS,
    )
    data["entity"].x = dataframe_to_tensor(
        entity_features,
        ENTITY_FEATURE_COLUMNS,
    )

    (
        data["account"].train_mask,
        data["account"].val_mask,
        data["account"].test_mask,
    ) = build_masks(
        data["account"].y,
        test_mode,
    )

    # Account → Account: transaction relation with 9 numeric edge features
    transaction_edge_index = make_edge_index(
        account_to_account_edges,
        "sender_account_id",
        "receiver_account_id",
        account_map,
        account_map,
    )

    transaction_edge_attributes = dataframe_to_tensor(
        account_to_account_edges,
        ACCOUNT_TO_ACCOUNT_EDGE_COLUMNS,
    )

    data["account", "transfers_to", "account"].edge_index = (
        transaction_edge_index
    )
    data["account", "transfers_to", "account"].edge_attr = (
        transaction_edge_attributes
    )

    # Reverse transaction relation, retaining the same edge attributes.
    data["account", "rev_transfers_to", "account"].edge_index = (
        transaction_edge_index.flip(0)
    )
    data["account", "rev_transfers_to", "account"].edge_attr = (
        transaction_edge_attributes
    )

    # Account → Bank: stable held_at relation, no numeric edge features
    account_bank_edge_index = make_edge_index(
        account_to_bank_edges,
        "account_id",
        "bank_id",
        account_map,
        bank_map,
    )

    data["account", "held_at", "bank"].edge_index = (
        account_bank_edge_index
    )
    data["bank", "rev_held_at", "account"].edge_index = (
        account_bank_edge_index.flip(0)
    )

    # Entity → Account: stable owns relation, no numeric edge features
    entity_account_edge_index = make_edge_index(
        entity_to_account_edges,
        "entity_id",
        "account_id",
        entity_map,
        account_map,
    )

    data["entity", "owns", "account"].edge_index = (
        entity_account_edge_index
    )
    data["account", "rev_owns", "entity"].edge_index = (
        entity_account_edge_index.flip(0)
    )

    id_maps = {
        "account_ids": account_ids,
        "bank_ids": bank_ids,
        "entity_ids": entity_ids,
    }

    return data, id_maps


def print_graph_summary(data: HeteroData) -> None:
    print("\nGraph created successfully.")
    print("Node feature dimensions:")

    for node_type in data.node_types:
        print(
            f"  {node_type}: {tuple(data[node_type].x.shape)}"
        )

    print("\nRelations:")

    for edge_type in data.edge_types:
        edge_count = data[edge_type].edge_index.shape[1]

        if hasattr(data[edge_type], "edge_attr"):
            edge_attribute_shape = tuple(
                data[edge_type].edge_attr.shape
            )
        else:
            edge_attribute_shape = "none"

        print(
            f"  {edge_type}: edges={edge_count:,}, "
            f"edge_attr={edge_attribute_shape}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the final MuleDetector HeteroData graph."
    )

    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Allow a graph with too few labels for a real train/val/test split.",
    )

    parser.add_argument(
        "--input-dir",
        type=Path,
        default=(PROCESSED_DATA_DIR / "spark_static_full"),
        help=(
            "Folder containing the final Spark Parquet outputs. "
            "Default: data/processed/spark_static_full"
        ),
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help=(
            "Folder for graph artifacts. Default: "
            "<input-dir>/graph_artifacts"
        ),
    )

    args = parser.parse_args()

    input_dir = args.input_dir.resolve()

    if not input_dir.is_dir():
        raise FileNotFoundError(
            f"Spark output directory does not exist: {input_dir}"
        )

    output_dir = (
        args.output_dir.resolve()
        if args.output_dir is not None
        else input_dir / "graph_artifacts"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Building MuleDetector heterogeneous graph...")
    print(f"Reading Spark outputs from: {input_dir}")

    data, id_maps = build_graph(
        test_mode=args.test_mode,
        input_dir=input_dir,
    )

    print_graph_summary(data)

    graph_file = output_dir / "mule_detector_graph.pt"
    id_maps_file = output_dir / "mule_detector_id_maps.pt"
    metadata_file = output_dir / "mule_detector_metadata.pt"

    torch.save(data, graph_file)
    torch.save(id_maps, id_maps_file)
    torch.save(
        {
            "account_feature_columns": ACCOUNT_FEATURE_COLUMNS,
            "bank_feature_columns": BANK_FEATURE_COLUMNS,
            "entity_feature_columns": ENTITY_FEATURE_COLUMNS,
            "account_to_account_edge_columns": (
                ACCOUNT_TO_ACCOUNT_EDGE_COLUMNS
            ),
            "node_feature_dimensions": {
                "account": len(ACCOUNT_FEATURE_COLUMNS),
                "bank": len(BANK_FEATURE_COLUMNS),
                "entity": len(ENTITY_FEATURE_COLUMNS),
            },
            "source_spark_output_dir": str(input_dir),
        },
        metadata_file,
    )

    print(f"\nSaved graph: {graph_file}")
    print(f"Saved ID maps: {id_maps_file}")
    print(f"Saved metadata: {metadata_file}")


if __name__ == "__main__":
    main()