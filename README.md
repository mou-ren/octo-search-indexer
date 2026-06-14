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
| `cmd/es-indexer/`     | Service entrypoint: Kafka consumer, offset/DLQ routing, /metrics |
| `internal/esindex/`   | Reusable ES bulk writer (imported by both the service and the phase-6 backfill job) |

> Current state: scaffold (package boundaries + signatures established,
> `go build ./...` / `go vet ./...` / `go test ./...` pass). The Kafka consumer
> and real OpenSearch bulk writer land in phase 4.

## Build

```sh
go build ./...
go test ./...
```

## License

[Apache License 2.0](./LICENSE).
