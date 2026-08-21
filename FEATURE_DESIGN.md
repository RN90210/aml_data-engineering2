# MuleDetector Feature Design — Phase 2

## Objective

Create a leakage-free heterogeneous graph for identifying mule accounts.
All model input features must be available using transaction history up to the
current event or micro-batch. `Is Laundering` is used only as the offline
training label and must never be a model input.

## Non-negotiable feature rules

1. `Is Laundering` is used only to create the offline account training label.
   It is never used in account, bank, entity, or edge input features.

2. Every feature is calculated using only transaction history available up to
   the current graph snapshot or mini-batch.

3. The same feature names, formulas, order, and normalization parameters
   will be used for offline training and later chronological inference.

4. Amount-based features use the transaction's recorded amount fields.
   Currency-switching and currency-diversity features are retained so the
   model can distinguish cross-currency behaviour.

5. Account-to-bank means `held_at` (the account's associated bank), not
   a temporary transaction-routing relationship.

## Graph nodes

- Account
- Bank
- Entity

## Graph relations

- Account → Account: transaction
- Account → Bank: belongs_to
- Entity → Account: owns
- Reverse relation for each relation above

## Account node features — 24 features

All features are calculated for one account using transactions observed up to the
current snapshot or mini-batch.

### Transaction activity

1. `outgoing_txn_count` — number of transactions sent by the account.
2. `incoming_txn_count` — number of transactions received by the account.
3. `outgoing_unique_counterparties` — number of distinct receiver accounts paid.
4. `incoming_unique_counterparties` — number of distinct sender accounts that paid it.
5. `repeat_counterparty_ratio` — total incoming and outgoing transaction count divided
   by total unique counterparties. Higher values indicate repeated interaction with a
   smaller network.
6. `self_transfer_ratio` — fraction of transactions where sender and receiver are the
   same account.

### Amount behaviour

7. `outgoing_total_paid` — total recorded `Amount Paid` for outgoing transactions.
8. `incoming_total_received` — total recorded `Amount Received` for incoming transactions.
9. `outgoing_avg_paid` — average recorded `Amount Paid` for outgoing transactions.
10. `incoming_avg_received` — average recorded `Amount Received` for incoming transactions.
11. `outgoing_amount_std` — standard deviation of outgoing recorded amounts.
12. `incoming_amount_std` — standard deviation of incoming recorded amounts.

Amount features are log-transformed before scaling. We do not use an incoming/outgoing
amount ratio because values may be recorded in different currencies.

### Currency and payment behaviour

13. `payment_currency_diversity` — number of distinct payment currencies used when sending.
14. `receiving_currency_diversity` — number of distinct receiving currencies used.
15. `currency_switch_ratio` — fraction of the account's transactions where payment and
    receiving currencies differ.
16. `payment_format_diversity` — number of distinct payment formats used.

### Network and bank behaviour

17. `unique_connected_banks` — number of distinct banks connected through incoming and
    outgoing transactions.
18. `cross_bank_txn_ratio` — fraction of transactions where `From Bank` and `To Bank`
    are different.

### Temporal and velocity behaviour

19. `odd_hour_txn_ratio` — fraction of transactions occurring from 00:00 to 05:59.
20. `weekend_txn_ratio` — fraction of transactions occurring on Saturday or Sunday.
21. `active_span_hours` — time in hours between the account's first and latest observed
    transaction.
22. `hours_since_last_txn` — time in hours from the account's latest transaction to the
    current graph snapshot time.
23. `max_txns_per_hour` — maximum number of transactions involving the account within
    any one-hour bucket.
24. `avg_inter_txn_gap_hours` — average elapsed time between consecutive transactions
    involving the account, ordered by timestamp.

## Account → Account transaction-edge features — 9 features

One directed edge represents the transaction relationship from sender account to
receiver account. All edge features are aggregated using transactions observed
up to the current snapshot or mini-batch.

1. `edge_txn_count` — number of transactions from this sender account to this
   receiver account.

2. `edge_total_paid` — total recorded `Amount Paid` across transactions on this
   directed relationship.

3. `edge_avg_paid` — average recorded `Amount Paid` across transactions on this
   directed relationship.

4. `edge_amount_std` — standard deviation of recorded `Amount Paid` across
   transactions on this directed relationship.

5. `edge_is_repeated` — `1` when this directed account pair has more than one
   transaction; otherwise `0`.

6. `edge_recency_hours` — elapsed hours from the latest transaction on this
   directed relationship to the current graph snapshot time.

7. `edge_currency_switch_ratio` — fraction of transactions on this relationship
   where `Payment Currency` differs from `Receiving Currency`.

8. `edge_payment_format_diversity` — number of distinct payment formats used on
   this relationship.

9. `edge_inter_txn_gap_hours` — average elapsed time between consecutive
   transactions on this directed relationship, ordered by timestamp.

`Is Laundering` is not stored as an edge feature. It is available only for
offline evaluation and account-label generation.

## Canonical identifiers and joins

### Account node ID

An account node is identified by the composite key:

`account_id = Bank ID + "::" + Account Number`

This prevents different banks with the same account number from being merged into
one graph node.

For each transaction:

- `sender_account_id = From Bank + "::" + Account`
- `receiver_account_id = To Bank + "::" + Account.1`

### Bank node ID

A bank node is identified by `Bank ID`.

### Entity node ID

An entity node is identified by `Entity ID`.

### Required metadata joins

The transactions table must be joined to the accounts table twice:

1. Sender metadata:
   `(From Bank, Account)` joined to `(Bank ID, Account Number)`

2. Receiver metadata:
   `(To Bank, Account.1)` joined to `(Bank ID, Account Number)`

This gives each transaction both sender and receiver bank/entity metadata.

## Stable metadata relations

### Account → Bank: `held_at`

Each account has one directed `held_at` relation to its associated bank.

- Source: `account_id`
- Destination: `Bank ID`
- Edge features: none

This is a stable metadata relationship, not an individual transaction route.

### Entity → Account: `owns`

Each entity has a directed `owns` relation to every account associated with it.

- Source: `Entity ID`
- Destination: `account_id`
- Edge features: none

This is also a stable metadata relationship. The entity and account node features
carry the behavioural information; the relation lets the graph model propagate
that context between them.

## Bank node features — 15 features

All bank features are calculated using transactions observed up to the current
snapshot or mini-batch. `Bank ID` is an identifier, not a model feature.

1. `bank_account_count` — number of accounts held at the bank, obtained from
   the accounts metadata.

2. `outgoing_txn_count` — number of transactions where this is the `From Bank`.

3. `incoming_txn_count` — number of transactions where this is the `To Bank`.

4. `outgoing_total_paid` — total recorded `Amount Paid` on outgoing transactions.

5. `incoming_total_received` — total recorded `Amount Received` on incoming
   transactions.

6. `outgoing_avg_paid` — average recorded `Amount Paid` on outgoing transactions.

7. `incoming_avg_received` — average recorded `Amount Received` on incoming
   transactions.

8. `unique_sender_accounts` — number of distinct accounts that sent transactions
   through this bank.

9. `unique_receiver_accounts` — number of distinct accounts that received
   transactions through this bank.

10. `counterparty_bank_count` — number of distinct other banks connected through
    incoming or outgoing transactions.

11. `currency_diversity` — number of distinct payment and receiving currencies
    observed for transactions touching this bank.

12. `payment_format_diversity` — number of distinct payment formats observed for
    transactions touching this bank.

13. `cross_bank_txn_ratio` — fraction of transactions touching this bank where
    `From Bank` and `To Bank` differ.

14. `odd_hour_txn_ratio` — fraction of transactions touching this bank occurring
    from 00:00 to 05:59.

15. `weekend_txn_ratio` — fraction of transactions touching this bank occurring
    on Saturday or Sunday.

Amount-based bank features are log-transformed before scaling. No bank feature
uses `Is Laundering` or any label-derived exposure score.

## Entity node features — 15 features

All entity features are aggregated over transactions involving accounts owned by
the entity, using history up to the current snapshot or mini-batch.

### Entity type — one-hot encoded

1. `entity_type_sole_proprietorship`
2. `entity_type_partnership`
3. `entity_type_corporation`
4. `entity_type_country`
5. `entity_type_other`

Exactly one entity-type feature is `1`; the others are `0`.

### Ownership and transaction behaviour

6. `owned_account_count` — number of accounts owned by the entity.

7. `connected_bank_count` — number of distinct banks associated with accounts
   owned by the entity.

8. `outgoing_txn_count` — number of transactions sent by accounts owned by
   the entity.

9. `incoming_txn_count` — number of transactions received by accounts owned by
   the entity.

10. `outgoing_total_paid` — total recorded `Amount Paid` across outgoing
    transactions of accounts owned by the entity.

11. `incoming_total_received` — total recorded `Amount Received` across incoming
    transactions of accounts owned by the entity.

12. `unique_counterparty_accounts` — number of distinct external accounts that
    transacted with any account owned by this entity.

13. `cross_bank_txn_ratio` — fraction of entity-related transactions where
    `From Bank` and `To Bank` differ.

14. `odd_hour_txn_ratio` — fraction of entity-related transactions occurring
    from 00:00 to 05:59.

15. `payment_format_diversity` — number of distinct payment formats used in
    entity-related transactions.

Amount-based entity features are log-transformed before scaling. No entity
feature uses `Is Laundering`, laundering exposure, or any target-derived value.

## Offline training label

`account_label = 1` when an account appears as either sender or receiver in at
least one transaction whose `Is Laundering` value is `1`.

The label is stored separately from all feature matrices. It is never used as a
node feature, edge feature, aggregation input, or inference input.

## Normalization contract

- Apply `log1p` to non-negative, heavy-tailed count and amount features.
- Keep ratio, binary, and one-hot features in their original form.
- Fit the scaler only on the training portion of the offline dataset.
- Save the fitted scaler parameters and feature order.
- During validation, test, and later mini-batch inference, load the saved
  parameters and transform only. Never fit a new scaler.