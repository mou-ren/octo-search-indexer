# octo-search-indexer

`es-indexer` — the OpenSearch indexer service for the OCTO message search pipeline.

It consumes the message body contract from Kafka topic `octo.message.v1`
(contract single-source-of-truth: [`octo-lib`](https://github.com/Mininglamp-OSS/octo-lib)
`contract/searchmsg`) and idempotently bulk-writes documents into OpenSearch
(`doc _id = message_id`), with Chinese tokenization handled by the index
mapping/analyzer.

## Where it sits in the pipeline

```
message 5-shard tables (octo-server, SoR)
  → searchetl producer (octo-server)        # increment keyset ETL → Kafka
  → Kafka topic octo.message.v1             # body contract (octo-lib/contract/searchmsg)
  → [ es-indexer  (THIS repo) ]             # consumer + ES bulk writer + 中文分词
  → OpenSearch
  → read path (octo-server)                 # query-side join to filter revoked/deleted
                                            #  + authz fail-CLOSED + paging
```

This is **phase 4** of the 9-phase YUJ-4530 message-search delivery
(YUJ-4534 umbrella). Lives in its own repo because the platform's CI/CD builds
one image per repository (one-repo-one-image), and `es-indexer` ships as an
independent binary/image distinct from `octo-server`.

## Design discipline

- **Decoupled reusable writer.** The Kafka consumer (`cmd/es-indexer`) and the
  ES writer (`internal/esindex`) are separated so the phase-6 backfill job can
  import `internal/esindex` and reuse the exact same write path
  (read `message` table → contract → `Writer.Bulk`) without copying ES code.
- **Idempotent sink.** ES `_id = message_id` (= Kafka key) gives
  effectively-once on top of an at-least-once delivery.
- **No revoked/deleted state in ES.** Route 甲: revoke/delete filtering happens
  at read time via MySQL join in `octo-server`. ES stores only body + the
  visibility fields required for query-side authz — matching the `searchmsg`
  contract.
- **Schema-version checked.** Unknown contract versions go to DLQ, never
  silently consumed.

## Layout

| Path                  | Purpose                                                        |
| --------------------- | ------------------------------------------------------------- |
| `cmd/es-indexer/`     | Service entrypoint: env config, graceful shutdown             |
| `internal/consumer/`  | Kafka consumer: FetchMessage + manual commit, ordered-prefix offset, DLQ routing + terminal escape (C4) |
| `internal/esindex/`   | Reusable ES bulk writer + index mapping bootstrap (imported by both the service and the phase-6 backfill job) |
| `internal/esindex/mapping/` | Canonical `octo-message` index mapping + 中文 analyzer (single source for the octo-deployment change) |

## Reliability semantics (C4)

- **Manual commit only.** The Reader uses `FetchMessage` + `CommitInterval=0`
  (no `ReadMessage` auto-commit). Offset advances **only to the contiguous
  success prefix** — the first transient failure stops the prefix, so Kafka's
  monotonic high-watermark commit can never silently confirm an unprocessed
  message.
- **Transient vs permanent.** 429 / 5xx / network / batch-level failures are
  transient → in-place backoff retry, offset not advanced. 4xx (except 429) and
  unknown `schema_version` are permanent poison pills → routed to the DLQ topic,
  then the offset crosses them.
- **DLQ terminal escape.** If the DLQ write itself keeps failing (transient),
  a bounded retry is followed by either local spill-to-disk + alarm + advance
  (when `INDEXER_DLQ_SPILL_DIR` is set), or a hard stop + page (when it is not).
  A DLQ outage can never wedge the prefix forever.
- **Idempotent sink.** `_id = message_id` → duplicate delivery upserts the same
  doc (effectively-once on top of at-least-once).

## Configuration (env)

| Variable | Default | Notes |
| -------- | ------- | ----- |
| `ES_INDEXER_ENABLED` | `false` | must be `true` AND brokers+ES set, else the binary idles (zero runtime effect) |
| `KAFKA_BROKERS` | — | CSV broker list |
| `KAFKA_TOPIC` | `octo.message.v1` | body topic |
| `KAFKA_DLQ_TOPIC` | `octo.message.v1.dlq` | poison-pill topic |
| `KAFKA_GROUP_ID` | `octo-search-indexer` | consumer group |
| `ES_ADDRESSES` | — | CSV OpenSearch node list |
| `ES_INDEX` | `octo-message` | target index |
| `ES_USERNAME` / `ES_PASSWORD` | — | HTTP basic auth |
| `INDEXER_BATCH_SIZE` | `500` | max docs per bulk |
| `INDEXER_TRANSIENT_BACKOFF_MS` | `1000` | retry backoff on transient |
| `INDEXER_DLQ_MAX_RETRIES` | `5` | DLQ write retries before escape |
| `INDEXER_DLQ_RETRY_BACKOFF_MS` | `200` | DLQ retry backoff base |
| `INDEXER_DLQ_SPILL_DIR` | — | set to enable spill escape; empty → hard-stop escape |

> Phase 4 delivered: Kafka consumer (C4), OpenSearch bulk writer, index mapping
> + 中文 analyzer bootstrap. `/metrics` and lag/backlog instrumentation land in
> phase 7.

## Backfill & reconciliation (must-run gate)

The phase-6 backfill job (`cmd/backfill`) loads existing `message` shards into
OpenSearch, bypassing Kafka, and is the **only** path that fills the reader's
safety fields (`spaceId`/`visibles`/`messageSeq`) from the raw MySQL payload.

A count match alone does not prove correctness — it proves only that the row
*counts* tie, not that each doc's authz/correctness fields are right. **Always run
the field-level reconciliation gate after a backfill:**

```sh
# inline after the backfill run (uses the job's own exact DLQ count as authority):
go run ./cmd/backfill ... -reconcile -from <epoch> -to <epoch>   # count gate + field-level sample gate

# or standalone, any time:
make run-recon RECON_FROM=<epoch> RECON_TO=<epoch> RECON_DLQ=<dlq-count>
```

The sample gate cross-checks `messageId` (full precision), `channelId`,
`channelType`, `spaceId`, and **`visibles`** between MySQL and the ES doc, so a
silently dropped ACL (`visibles`) field surfaces as `sample_mismatch>0` and the
gate exits non-zero. `make run-recon` is **mandatory** before switching the read
alias to a freshly backfilled index (see `docs/forward-migration-v1.9.md`).

## Build

```sh
go build ./...
go test ./...
```

## License

[Apache License 2.0](./LICENSE).
