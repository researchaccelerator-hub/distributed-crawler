# Random Walk Crawl Architecture

This document describes the random walk crawling process used by the distributed Telegram crawler. Two modes are supported:

- **Standard mode** — the crawler validates discovered channels itself via TDLib `SearchPublicChat` RPCs
- **Tandem mode** (`--tandem-crawl`) — the crawler writes discovered edges to a `pending_edges` table; a separate validator pod validates them via HTTP (`t.me/<username>`) to avoid account-level Telegram rate limits

Both modes share the same core loop (`RunRandomWalkLayerless`), page buffer, edge recording, and walkback logic. They diverge at the point where discovered outlinks are validated and the next page is selected.

---

## 1. System Overview

The highest level of abstraction — how the major components interact.

### 1.1 Standard Mode

```mermaid
graph TB
    subgraph Crawler Pod
        LOOP[RunRandomWalkLayerless]
        POOL[TDLib Connection Pool]
        PROC[processAllMessagesWithProcessor]
    end

    subgraph PostgreSQL
        PB[(page_buffer)]
        ER[(edge_records)]
        SC[(seed_channels)]
        IC[(invalid_channels)]
        DC[(discovered_channels)]
    end

    LOOP -->|ClaimPages| PB
    LOOP -->|dispatch| POOL
    POOL -->|RunForChannel| PROC
    PROC -->|SearchPublicChat| TG[Telegram API]
    PROC -->|InsertEdgeRecord| ER
    PROC -->|AddPageToPageBuffer| PB
    PROC -->|MarkChannelCrawled| SC
    PROC -->|MarkChannelInvalid| IC
    PROC -->|ClaimDiscoveredChannel| DC
    LOOP -->|DeletePageBufferPages| PB
    LOOP -->|RecoverStalePageClaims| PB
```

### 1.2 Tandem Mode

```mermaid
graph TB
    subgraph Crawler Pod
        LOOP[RunRandomWalkLayerless]
        POOL[TDLib Connection Pool]
        PROC[processAllMessagesWithProcessor]
    end

    subgraph Validator Pod
        VLOOP[RunValidationLoop]
        EVAL[runEdgeValidation]
        WB[runWalkbackProcessor]
    end

    subgraph PostgreSQL
        PB[(page_buffer)]
        ER[(edge_records)]
        PE[(pending_edges)]
        PEB[(pending_edge_batches)]
        SC[(seed_channels)]
        IC[(invalid_channels)]
        DC[(discovered_channels)]
    end

    LOOP -->|ClaimPages| PB
    LOOP -->|dispatch| POOL
    POOL -->|RunForChannel| PROC
    PROC -->|CreatePendingBatch| PEB
    PROC -->|InsertPendingEdge| PE
    PROC -->|ClosePendingBatch| PEB

    EVAL -->|ClaimPendingEdges| PE
    EVAL -->|HTTP GET t.me/username| TME[t.me]
    EVAL -->|UpdatePendingEdge| PE
    EVAL -->|ClaimDiscoveredChannel| DC
    EVAL -->|MarkChannelInvalid| IC

    WB -->|ClaimWalkbackBatch| PEB
    WB -->|InsertEdgeRecord| ER
    WB -->|AddPageToPageBuffer| PB
    WB -->|CompletePendingBatch| PEB

    LOOP -->|CountIncompleteBatches| PEB
    LOOP -->|DeletePageBufferPages| PB
```

---

## 2. Core Loop — RunRandomWalkLayerless

This is the engine shared by both modes. It runs in `dapr/standalone.go` (line 888).

```mermaid
flowchart TD
    START([Start]) --> INIT["Initialize semaphore<br/>Start stale-claim recovery goroutine"]
    INIT --> CLAIM["ClaimPages from page_buffer<br/>FOR UPDATE SKIP LOCKED"]

    CLAIM -->|pages found| DISPATCH["For each page:<br/>acquire semaphore slot"]
    CLAIM -->|no pages| EMPTY{Tandem mode?}

    EMPTY -->|yes| TANDEM_CHECK{"inFlightCount == 0<br/>AND<br/>CountIncompleteBatches == 0?"}
    EMPTY -->|no| STD_CHECK{inFlightCount == 0?}

    STD_CHECK -->|yes| DONE([Crawl complete])
    STD_CHECK -->|no| WAIT[Sleep 5s] --> CLAIM

    TANDEM_CHECK -->|yes| DONE
    TANDEM_CHECK -->|no| TIMEOUT{"ValidatorTimeout<br/>exceeded?"}
    TIMEOUT -->|yes| ABORT([Abort: validator blocked])
    TIMEOUT -->|no| WAIT

    DISPATCH --> WORKER["goroutine:<br/>RunForChannelWithPool"]
    WORKER --> RESULT{Result?}

    RESULT -->|success| DELETE[DeletePageBufferPages]
    RESULT -->|ErrWalkbackExhausted| UNCLAIM["UnclaimPages<br/>leave for retry"]
    RESULT -->|ErrFloodWaitRetire| RETIRE["RetireConnection<br/>UnclaimPages"]
    RESULT -->|ErrTDLib400| REPLACE[Handle400Replacement]

    RETIRE --> POOL_CHECK{Pool empty?}
    POOL_CHECK -->|yes| ABORT
    POOL_CHECK -->|no| RELEASE[Release semaphore]

    DELETE --> RELEASE
    UNCLAIM --> RELEASE
    REPLACE --> RELEASE

    RELEASE --> CLAIM

    style DONE fill:#2d6,stroke:#333
    style ABORT fill:#d33,stroke:#333
```

### Key design decisions

- **Semaphore concurrency**: Each worker holds a semaphore slot, not a batch-barrier. Pages are deleted immediately per-worker — no convoy effect.
- **Stale claim recovery**: A background goroutine runs `RecoverStalePageClaims()` every 5 minutes, releasing pages claimed by crashed pods.
- **Tandem completion**: The crawler only exits when both `inFlightCount == 0` (all workers done) AND `CountIncompleteBatches == 0` (validator finished all batches).

---

## 3. Channel Processing — RunForChannel

Runs per-page inside a worker goroutine. Acquires a TDLib connection from the pool, fetches messages, extracts outlinks.

```mermaid
flowchart TD
    START([Page claimed]) --> CONN[GetConnectionFromPool]
    CONN --> INFO["getChannelInfoWithDeps<br/>cached chat_id or SearchPublicChat"]
    INFO -->|error| ERR400{TDLib 400?}
    ERR400 -->|yes| RET400([return ErrTDLib400])
    ERR400 -->|no| RETERR([return error])

    INFO -->|success| ACTIVE{"Channel active<br/>within period?"}
    ACTIVE -->|no| SKIP["Mark walkback edge<br/>Add walkback page to buffer"]
    ACTIVE -->|yes| FETCH["FetchMessages<br/>since last_crawled_at"]

    FETCH --> STORE[StorePost — write .jsonl]
    STORE --> EXTRACT["ExtractChannelLinksWithSource<br/>from each message"]

    EXTRACT --> MODE{Tandem mode?}

    MODE -->|no| STANDARD["Standard edge processing<br/>SearchPublicChat validation"]
    MODE -->|yes| TANDEM["Tandem edge processing<br/>Write to pending_edges"]

    STANDARD --> WALKBACK[Walkback decision]
    TANDEM --> WALKBACK

    WALKBACK --> MARK["MarkChannelCrawled<br/>Update seed_channels.last_crawled_at"]
    MARK --> RELEASE[ReleaseConnectionToPool]
    RELEASE --> DONE([return nil])

    SKIP --> RELEASE

    style DONE fill:#2d6,stroke:#333
    style RET400 fill:#d93,stroke:#333
    style RETERR fill:#d33,stroke:#333
```

---

## 4. Edge Processing — Standard Mode

When `--tandem-crawl` is NOT set. The crawler validates outlinks itself via TDLib.

```mermaid
flowchart TD
    START([Outlinks extracted]) --> LOOP[For each outlink username]

    LOOP --> INVALID{"In invalid_channels<br/>cache?"}
    INVALID -->|yes| SKIP_LINK[Skip] --> LOOP
    INVALID -->|no| DISCOVERED{"Already in<br/>discovered_channels?"}
    DISCOVERED -->|yes| SKIP_LINK
    DISCOVERED -->|no| SEARCH[SearchPublicChat via TDLib]

    SEARCH -->|FLOOD_WAIT| FLOOD{Duration > 300s?}
    FLOOD -->|yes| RETIRE([return ErrFloodWaitRetire])
    FLOOD -->|no| SLEEP[Sleep duration] --> SEARCH

    SEARCH -->|not found / not supergroup| MARK_INV[MarkChannelInvalid] --> LOOP
    SEARCH -->|success| CACHE["UpsertSeedChannelChatID<br/>AddDiscoveredChannel"]

    CACHE --> ADD[Add to newChannels map]
    ADD --> LOOP

    LOOP -->|all processed| DECIDE{"newChannels<br/>count > 0?"}
    DECIDE -->|no| FORCED_WB[Forced walkback]
    DECIDE -->|yes| ROLL["rand 1-100<br/>vs WalkbackRate"]

    ROLL -->|walkback| DO_WB["pickWalkbackChannel<br/>new SequenceID"]
    ROLL -->|forward| PICK["Pick random from newChannels<br/>keep SequenceID"]

    FORCED_WB --> WRITE_EDGE
    DO_WB --> WRITE_EDGE
    PICK --> WRITE_EDGE

    WRITE_EDGE["Write edge_records:<br/>1 primary + N skipped edges"]
    WRITE_EDGE --> SEED_PAGE["AddPageToPageBuffer<br/>next channel"]

    style RETIRE fill:#d33,stroke:#333
```

### Edge record fields

| Field | Forward walk | Walkback | Skipped |
|-------|-------------|----------|---------|
| `walkback` | false | true | false |
| `skipped` | false | false | true |
| `sequence_id` | parent's | parent's | parent's |

The **next page** inherits the parent's `sequence_id` on forward walk, or gets a **new UUID** on walkback (breaking the chain).

---

## 5. Edge Processing — Tandem Mode

When `--tandem-crawl` IS set. The crawler writes raw outlinks; a separate validator pod processes them.

### 5.1 Crawler side

```mermaid
flowchart TD
    START([Outlinks extracted]) --> LOOP[For each outlink username]

    LOOP --> FILTER["FilterUsername<br/>reject bots, reserved paths"]
    FILTER -->|rejected| LOOP
    FILTER -->|ok| DEDUP{"Seen in this<br/>batch already?"}
    DEDUP -->|yes| LOOP
    DEDUP -->|no| BATCH{"Batch<br/>created?"}
    BATCH -->|no| CREATE["CreatePendingBatch<br/>status=open"] --> INSERT
    BATCH -->|yes| INSERT["InsertPendingEdge<br/>status=pending"]
    INSERT --> LOOP

    LOOP -->|all processed| CLOSE["ClosePendingBatch<br/>status=closed"]
    CLOSE --> EDGES{"Any edges<br/>in batch?"}
    EDGES -->|no| FORCED_WB["Forced walkback by crawler<br/>pickWalkbackChannel<br/>write edge_record + page directly"]
    EDGES -->|yes| RETURN([Return — validator handles walkback])

    style RETURN fill:#2d6,stroke:#333
```

### 5.2 Validator side — Edge Validation

```mermaid
flowchart TD
    START([runEdgeValidation goroutine]) --> BLOCKED{"In blocked<br/>state?"}

    BLOCKED -->|yes| PROBE["Probe t.me/telegram<br/>every 5 min"]
    PROBE -->|still blocked| BLOCKED
    PROBE -->|unblocked| CLAIM

    BLOCKED -->|no| CLAIM["ClaimPendingEdges<br/>FOR UPDATE SKIP LOCKED"]
    CLAIM -->|none| SLEEP[Sleep 2s] --> START

    CLAIM -->|edges| LOOP[For each edge]
    LOOP --> INV{In invalid_channels?}
    INV -->|yes| UPDATE_INV[status=invalid] --> LOOP
    INV -->|no| DISC{Already discovered?}
    DISC -->|yes| UPDATE_DUP[status=duplicate] --> LOOP
    DISC -->|no| RATE[rateLimiter.Wait]

    RATE --> HTTP["HTTP GET t.me/username<br/>uTLS Chrome fingerprint"]
    HTTP --> RESULT{Response?}

    RESULT -->|valid channel| CLAIM_DISC["ClaimDiscoveredChannel<br/>INSERT ON CONFLICT DO NOTHING"]
    CLAIM_DISC -->|won race| UPDATE_VALID["status=valid<br/>cache chat_id"]
    CLAIM_DISC -->|lost race| UPDATE_DUP
    RESULT -->|not channel / invalid| MARK["MarkChannelInvalid<br/>status=not_channel or invalid"]
    RESULT -->|blocked by t.me| ENTER_BLOCKED["Enter blocked state<br/>write access_event"]
    RESULT -->|transient error| UPDATE_PEND["Reset to status=pending<br/>for retry"]

    UPDATE_VALID --> LOOP
    MARK --> LOOP
    UPDATE_PEND --> LOOP

    LOOP -->|done| START
```

### 5.3 Validator side — Walkback Processing

```mermaid
flowchart TD
    START([runWalkbackProcessor goroutine]) --> STALE["Every 5 min:<br/>RecoverStaleBatchClaims<br/>RecoverStaleValidatingEdges"]
    STALE --> CLAIM["ClaimWalkbackBatch<br/>oldest batch where status=closed<br/>AND no pending/validating edges"]

    CLAIM -->|none| SLEEP[Sleep 5s] --> START
    CLAIM -->|batch + edges| COLLECT["Collect valid first-claimed<br/>channels from edges"]

    COLLECT --> COUNT{"Valid channel<br/>count > 0?"}
    COUNT -->|no| FORCED_WB["Forced walkback<br/>pickWalkbackChannel"]
    COUNT -->|yes| ROLL["rand 1-100<br/>vs WalkbackRate"]

    ROLL -->|walkback| DO_WB["pickWalkbackChannel<br/>new SequenceID"]
    ROLL -->|forward| PICK["Pick random valid channel<br/>keep SequenceID"]

    FORCED_WB --> WRITE
    DO_WB --> WRITE
    PICK --> WRITE

    WRITE["Write edge_records:<br/>1 primary + N skipped<br/>AddPageToPageBuffer"]
    WRITE --> COMPLETE["CompletePendingBatch<br/>status=completed"]
    COMPLETE --> STATS["FlushBatchStats<br/>to source_type_stats"]
    STATS --> START
```

---

## 6. Walkback Decision Logic

The walkback mechanism prevents the walk from getting stuck in dense clusters by periodically jumping to a previously-seen channel.

```mermaid
flowchart TD
    EDGES_DONE([All edges processed]) --> HAS_NEW{newChannelCount > 0?}

    HAS_NEW -->|no| FORCED[Forced walkback]
    HAS_NEW -->|yes| ROLL[roll = rand 1..100]

    ROLL --> CMP{roll <= WalkbackRate?}
    CMP -->|yes| VOLUNTARY[Voluntary walkback]
    CMP -->|no| FORWARD[Forward walk]

    FORCED --> PICK_WB
    VOLUNTARY --> PICK_WB

    PICK_WB["pickWalkbackChannel<br/>up to 10 attempts<br/>exclude source + current edges"]
    PICK_WB -->|found| NEW_SEQ["Generate new SequenceID<br/>Edge: walkback=true"]
    PICK_WB -->|exhausted| ERR(["ErrWalkbackExhausted<br/>page unclaimed for retry"])

    FORWARD --> PICK_FWD[Pick random from newChannels]
    PICK_FWD --> SAME_SEQ["Keep parent SequenceID<br/>Edge: walkback=false"]

    NEW_SEQ --> RECORD
    SAME_SEQ --> RECORD

    RECORD["Write to edge_records:<br/>- 1 primary edge<br/>- N-1 skipped edges from newChannels"]
    RECORD --> PAGE["AddPageToPageBuffer<br/>next channel to crawl"]

    style ERR fill:#d93,stroke:#333
```

### Sequence chain example

```mermaid
sequenceDiagram
    participant PB as page_buffer
    participant C as Crawler
    participant ER as edge_records

    Note over PB: Page A (seq=α)
    C->>PB: ClaimPages → A
    C->>C: Crawl channel A → discovers B, C, D
    C->>C: Forward walk → pick B

    C->>ER: edge(A→B, seq=α, walkback=false)
    C->>ER: edge(A→C, seq=α, skipped=true)
    C->>ER: edge(A→D, seq=α, skipped=true)
    C->>PB: Add Page B (seq=α)

    Note over PB: Page B (seq=α)
    C->>PB: ClaimPages → B
    C->>C: Crawl channel B → discovers E
    C->>C: Walkback triggered → pick random discovered

    C->>ER: edge(B→X, seq=α, walkback=true)
    C->>PB: Add Page X (seq=β ← new chain)

    Note over PB: Page X (seq=β)
    C->>PB: ClaimPages → X
    C->>C: New chain begins...
```

---

## 7. Error Handling & Recovery

```mermaid
flowchart TD
    subgraph "Per-worker error handling"
        ERR{Error type?}
        ERR -->|nil| OK["DeletePageBufferPages<br/>release semaphore"]
        ERR -->|ErrWalkbackExhausted| WBE["UnclaimPages<br/>page stays in buffer for retry"]
        ERR -->|ErrFloodWaitRetire| FW["RetireConnection from pool<br/>UnclaimPages"]
        ERR -->|ErrTDLib400| E400[Handle400Replacement]
    end

    FW --> POOL_EMPTY{"Connection<br/>pool empty?"}
    POOL_EMPTY -->|yes| ABORT([Abort crawl])
    POOL_EMPTY -->|no| CONTINUE[Continue]

    subgraph "Handle400Replacement"
        E400 --> KIND{Edge kind?}
        KIND -->|seed channel| SEED["GetRandomSeedChannels<br/>replace page in buffer"]
        KIND -->|walkback| WB400["pickWalkbackChannel<br/>replace page in buffer"]
        KIND -->|forward| SKIPPED{"Has skipped edges<br/>in same sequence?"}
        SKIPPED -->|yes| PROMOTE["PromoteEdge<br/>replace page in buffer"]
        SKIPPED -->|no| WB400
    end

    subgraph "Background recovery"
        STALE["RecoverStalePageClaims<br/>every 5 min"]
        STALE --> UNCLAIM_OLD["Unclaim pages where<br/>claimed_at > 15 min ago"]
    end

    style ABORT fill:#d33,stroke:#333
```

---

## 8. Database Schema Relationships

```mermaid
erDiagram
    page_buffer {
        varchar page_id PK
        varchar parent_id
        int depth
        varchar url
        varchar crawl_id
        varchar sequence_id
        varchar claimed_by
        timestamp claimed_at
    }

    edge_records {
        serial edge_id PK
        varchar destination_channel
        varchar source_channel
        boolean walkback
        boolean skipped
        timestamp discovery_time
        varchar crawl_id
        varchar sequence_id
    }

    seed_channels {
        varchar channel_username PK
        bigint chat_id
        timestamp last_crawled_at
        timestamp invalidated_at
        int member_count
    }

    invalid_channels {
        varchar channel_username PK
        varchar reason
        timestamp invalidated_at
    }

    pending_edge_batches {
        varchar batch_id PK
        varchar crawl_id
        varchar source_channel
        varchar source_page_id
        int source_depth
        varchar sequence_id
        varchar status
        int attempt_count
        timestamp created_at
        timestamp closed_at
        timestamp claimed_at
        timestamp completed_at
    }

    pending_edges {
        serial pending_id PK
        varchar batch_id FK
        varchar crawl_id
        varchar destination_channel
        varchar source_channel
        varchar sequence_id
        timestamp discovery_time
        varchar source_type
        varchar validation_status
        varchar validation_reason
        timestamp validated_at
    }

    discovered_channels {
        varchar channel_username PK
        varchar crawl_id
        timestamp discovered_at
    }

    source_type_stats {
        varchar crawl_id PK
        varchar source_type PK
        int total
        int valid
        int not_channel
        int invalid
        int duplicate
    }

    access_events {
        serial id PK
        varchar reason
        timestamptz occurred_at
    }

    pending_edge_batches ||--o{ pending_edges : "batch_id"
    page_buffer }o--|| edge_records : "feeds next page"
    pending_edges }o--|| discovered_channels : "validated → claimed"
    pending_edge_batches }o--|| edge_records : "walkback writes"
    pending_edge_batches }o--|| source_type_stats : "FlushBatchStats"
```

---

## 9. Multi-Pod Concurrency

Multiple crawler pods share the same `page_buffer` and `crawl_id`. Isolation is achieved via PostgreSQL row-level locking.

```mermaid
sequenceDiagram
    participant P1 as Pod crawler-0
    participant P2 as Pod crawler-1
    participant DB as PostgreSQL page_buffer

    par Concurrent claims
        P1->>DB: UPDATE ... SET claimed_by='crawler-0' WHERE ... FOR UPDATE SKIP LOCKED LIMIT 5
        P2->>DB: UPDATE ... SET claimed_by='crawler-1' WHERE ... FOR UPDATE SKIP LOCKED LIMIT 5
    end

    Note over DB: SKIP LOCKED ensures no two pods claim the same page

    DB-->>P1: Pages [A, B, C]
    DB-->>P2: Pages [D, E]

    P1->>P1: Process A, B, C
    P2->>P2: Process D, E

    P1->>DB: DELETE pages [A, B, C]
    P2->>DB: DELETE pages [D, E]

    Note over DB: Both pods write new pages — ON CONFLICT DO NOTHING prevents duplicate seeds
```

---

## Appendix: Configuration Flags

| Flag | Default | Mode | Purpose |
|------|---------|------|---------|
| `--sampling-method random-walk` | — | both | Enables random walk mode |
| `--tandem-crawl` | false | tandem | Write to pending_edges instead of SearchPublicChat |
| `--validate-only` | false | validator | Run as validator pod only |
| `--walkback-rate` | 20 | both | Percentage chance of voluntary walkback (1-100) |
| `--concurrency` | 1 | both | Number of parallel workers / TDLib connections |
| `--validator-request-rate` | 6 | validator | HTTP calls per minute |
| `--validator-claim-batch-size` | 10 | validator | Edges claimed per DB round-trip |
| `--validator-timeout` | 0 | tandem | Abort if validator blocked for this duration (0=disabled) |
| `--exit-on-complete` | false | both | Exit pod when crawl finishes |
