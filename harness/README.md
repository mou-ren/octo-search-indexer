# Local e2e verification harness

A throwaway local stack that runs the **whole** message-search pipeline for real:

```
seed → Kafka octo.message.v1 → es-indexer consumer → OpenSearch (analysis-ik)
                                      └→ octo.message.v1.dlq (poison pills)
```

> 🔒 **Isolation.** This is a local-only stack (`docker compose`). It is NEVER
> wired into a shared test environment, and the indexer's `Kafka.On` / real
> deployment wiring stays off until a separate, explicitly signed-off step.
> Real deployment provisioning (octo-deployment, IK dictionaries, topics) is out
> of scope here.

## Contents

| Path | What |
| --- | --- |
| `docker-compose.yml` | Kafka (KRaft, 1 broker) + OpenSearch (1 node) |
| `opensearch-ik.Dockerfile` | OpenSearch image with the `analysis-ik` plugin installed (version-matched) |
| `run.sh` | orchestrator: `up` (build+start+create topics) / `down` / `seed` / `verify` / `all` |
| `seed/` | producer of a controlled message suite (normal / 中文 / raw_excluded / duplicate _id / unknown schema / bad JSON) and a `-mode bulk -n N` throughput load |
| `verify/` | asserts the invariants against OpenSearch |

## Run it

```sh
# full cycle: up → indexer → seed suite → verify invariants → down
./harness/run.sh

# or step by step, leaving the stack up:
KEEP_UP=1 ./harness/run.sh up
ES_INDEXER_ENABLED=true KAFKA_BROKERS=localhost:19092 \
  ES_ADDRESSES=http://localhost:19200 ES_INDEX=octo-message \
  KAFKA_DLQ_TOPIC=octo.message.v1.dlq INDEXER_DLQ_SPILL_DIR=/tmp/spill \
  go run ./cmd/es-indexer &        # start the indexer against the harness
go run ./harness/seed -mode suite  # seed the controlled suite
go run ./harness/verify            # assert invariants
./harness/run.sh down
```

Ports: Kafka `localhost:19092`, OpenSearch `http://localhost:19200`.

## Invariants asserted (`verify/`)

- **Idempotency** — duplicate `message_id` → exactly one ES doc (`_id` upsert).
- **C4 schema gate** — unknown `schema_version` and malformed JSON are NOT
  indexed (routed to the DLQ topic).
- **raw_excluded** — Signal/non-text doc IS indexed (content null), occupies a doc.
- **IK tokenization** — Chinese terms (`公园`, `北京`) recall their docs; English
  (`pipeline`) too.

The C2/C4 producer/consumer state-machine invariants (per-partition ordered
commit, transient in-place retry, DLQ terminal escape) are unit-tested under
`-race` in `internal/consumer`; the harness exercises the same code paths against
a real broker + real OpenSearch.

## Notes

- The DLQ topic must be **pre-created** (the indexer's DLQ producer uses
  `AllowAutoTopicCreation=false` by design). `run.sh up` creates both topics; in
  production the platform provisions them.
- The harness OpenSearch disables the security plugin for plain-HTTP local
  testing only — production uses authenticated endpoints (`ES_USERNAME`/`ES_PASSWORD`).
