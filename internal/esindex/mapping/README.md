# octo-message index mapping

`octo-message.json` is the **canonical index mapping + analyzer** the
`es-indexer` writes against, embedded into the binary (`//go:embed`) and used by
`esindex.EnsureIndex` to bootstrap the index idempotently when it does not yet
exist.

It is the single source the **octo-deployment** coordinated change references:
provisioning/managed-OpenSearch templates must stay in lockstep with this file
at the same contract version (`searchmsg.SchemaVersion`).

## Field rationale (route 甲: body + query-side authz visibility only)

| Field            | Type                | Why                                                        |
| ---------------- | ------------------- | --------------------------------------------------------- |
| `message_id`     | `keyword`           | = ES `_id` / Kafka key; exact-match dedupe key            |
| `channel_id`     | `keyword`           | query-side authz filter (group/topic/DM resolution)       |
| `channel_type`   | `integer`           | authz routing per channel type                            |
| `from_uid`       | `keyword`           | sender filter / authz                                     |
| `content`        | `text` + analyzer   | the searchable body — Chinese tokenization                |
| `content_type`   | `integer`           | message type                                              |
| `raw_excluded`   | `boolean`           | Signal/non-text marker (content null)                     |
| `msg_timestamp`  | `long`              | send time (epoch s)                                       |
| `created_at`     | `long`              | landed time (epoch s)                                     |
| `source`         | `keyword`           | provenance (ETL vs future CDC)                            |
| `schema_version` | `integer`           | contract version stamped on every doc                     |

No `revoked` / `deleted` fields — route 甲 filters those at read time via MySQL
join. Mapping is `dynamic: strict` so any unexpected field fails loudly rather
than silently polluting the index.

## Chinese tokenization

Ships with a built-in `cjk_bigram`-based analyzer (`octo_cjk_text`) so the
service has a working zero-dependency default. If the managed OpenSearch cluster
provides a dictionary tokenizer plugin (IK / smartcn), the octo-deployment
change can override `content.analyzer` there — that selection is owned by the
deployment change, not pinned here.
