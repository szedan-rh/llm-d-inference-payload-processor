# Streaming Token Counting — Design Specification

Issue: [praxis-proxy/praxis#211](https://github.com/praxis-proxy/praxis/issues/211)
Epic: [praxis-proxy/praxis#20 — Token Counting](https://github.com/praxis-proxy/praxis/issues/20)

## Design Status

***Proposed***

## Summary

A `ResponseProcessor` plugin that parses Server-Sent Event (SSE) streams from LLM inference responses, extracts token usage counts using provider-specific strategies, and writes aggregated counts to `CycleState` for downstream consumers. The design applies Rust-inspired principles — strong ownership boundaries, zero-copy parsing, composition over inheritance, and exhaustive type matching — within the existing Go plugin architecture.

## Background: What is SSE?

Server-Sent Events (SSE) is a protocol for streaming data from server to client over HTTP. Instead of returning one JSON response, the LLM backend sends a series of small text events separated by blank lines:

```
data: {"id":"chatcmpl-abc","choices":[{"delta":{"content":"Hello"}}],"usage":{"prompt_tokens":10,"completion_tokens":1}}

data: {"id":"chatcmpl-abc","choices":[{"delta":{"content":" world"}}],"usage":{"prompt_tokens":10,"completion_tokens":2}}

data: [DONE]
```

Each `data:` line carries a chunk of generated text and sometimes incremental token usage counters. The stream terminates with a sentinel (`data: [DONE]` for OpenAI, `event: message_stop` for Anthropic, etc.).

Most production AI inference uses streaming mode. Without parsing these SSE events, token counting only works for non-streaming requests — leaving the majority of traffic unmetered.

## Approaches Considered

### Approach 1: Incremental (Per-Chunk Parsing)

Parse SSE events inside the `server.go` recv loop as each chunk arrives from Envoy, before accumulation completes.

```
for {
    chunk := srv.Recv()
    responseBody = append(responseBody, chunk...)
    sseParser.Feed(chunk)                    // parse incrementally
    for _, event := range sseParser.Drain() {
        tokenAccum.Process(event)            // count in real-time
    }
    if !EndOfStream { continue }
    // final counts available immediately
}
```

**Pros:**
- Real-time token visibility — counts available mid-stream
- Enables early abort (kill stream if token quota exceeded)
- Lower peak memory — parsed events can be discarded after extraction

**Cons:**
- Requires modifying the core recv loop in `server.go` — the heart of the proxy
- Stateful parser lives in the hot path — a bug crashes all requests
- Chunk boundaries don't align with SSE event boundaries — requires complex partial-line buffering
- Tight coupling between SSE parsing and the Envoy gRPC protocol layer
- Testing requires mocking the full gRPC stream, not just feeding bytes

### Approach 2: Post-Accumulation (Parse After All Chunks Received)

Keep the current accumulation pattern unchanged. After `EndOfStream`, re-parse the full accumulated bytes as SSE in `HandleResponseBody` before passing to plugins.

```go
func (s *Server) HandleResponseBody(...) {
    if isSSEResponse(headers) {
        events := sse.ParseAll(responseBodyBytes)
        usage := accumulate(events)
        cycleState.Write("token_usage", usage)
    }
    // existing plugin pipeline continues...
}
```

**Pros:**
- Zero changes to the core recv loop — safest option
- All SSE events are complete — no partial-line edge cases
- Easy to test — feed a byte slice, assert on output
- Fits cleanly into the existing `HandleResponseBody` flow

**Cons:**
- Doubles peak memory — full raw bytes + parsed event objects simultaneously
- Token counts only available after the entire stream finishes
- Cannot support future features like mid-stream rate limiting
- Wasteful for the common case where only the last chunk has usage data

### Approach 3: Hybrid (Recommended)

Minimal work per-chunk (accumulate bytes as today), but add a new `ResponseProcessor` plugin that understands SSE format and extracts tokens from the accumulated bytes after `EndOfStream`.

```
server.go recv loop:  unchanged — just accumulate bytes
HandleResponseBody:   detect SSE content-type, store raw bytes in CycleState
Plugin:               parse SSE → extract tokens → write TokenUsage to CycleState
```

**Pros:**
- No changes to the core recv loop — preserves stability
- SSE parsing isolated in a plugin — follows the existing plugin architecture
- Plugin is independently testable — feed bytes, assert on token counts
- Composable — provider-specific parsers are swappable via interfaces (Strategy pattern)
- Follows Rust's separation of concerns — the handler owns transport, the plugin owns semantics
- Other plugins consume token counts via CycleState — loose coupling
- Adding a new provider means adding one struct, not touching existing code (Open/Closed Principle)

**Cons:**
- Still waits for full accumulation — no mid-stream token decisions
- Slightly more complex than approach 2 (new plugin + SSE parser)

**Why Hybrid wins:** It respects the existing architecture (no core loop surgery), isolates SSE complexity behind clean interfaces, and is the most testable. The incremental approach is more powerful but the risk/reward ratio is unfavorable for a proxy that handles all inference traffic. Post-accumulation is simpler but puts parsing logic in the handler rather than the plugin system where it belongs.

## Detailed Design

### Design Principles (Rust-Inspired)

1. **Ownership boundaries** — each type owns its data. State flows in one direction: raw bytes → parsed events → token counts. No shared mutable references.
2. **Zero-copy where possible** — SSE event data stored as `json.RawMessage` (a `[]byte` alias), deferring deserialization until the provider-specific extractor needs it.
3. **Composition over inheritance** — the pipeline is assembled from small, single-purpose interfaces. No deep type hierarchies.
4. **Exhaustive type matching** — provider extractors handle every known event type explicitly. Unknown types are logged, not silently dropped.
5. **Errors are values** — every failure mode has a named metric and a clear recovery path. No panics. Token counting failures never break the response path.

### Core Types

```go
// TokenUsage is a value type — immutable once constructed.
// Represents the final aggregated token counts for a single request.
type TokenUsage struct {
    PromptTokens     int64
    CompletionTokens int64
    TotalTokens      int64
}

// SSEEvent represents a single parsed SSE event.
// The parser produces these, the extractor consumes them.
// No shared mutable state between producer and consumer.
type SSEEvent struct {
    Type string            // e.g. "message_start", "message_delta", ""
    Data json.RawMessage   // raw JSON — zero-copy, deferred deserialization
}
```

**Ownership flow:**

```
raw bytes ──owns──▶ SSEParser ──produces──▶ []SSEEvent ──consumed by──▶ TokenExtractor ──produces──▶ TokenUsage
                    (owns buffer)           (value types)               (owns strategy)              (written to CycleState)
```

### Interface Design

Three interfaces compose into the pipeline. Each has a single responsibility — narrow and composable, like Rust traits.

#### SSEParser — byte stream to structured events

```go
type SSEParser interface {
    Parse(raw []byte) ([]SSEEvent, error)
}
```

One implementation. Stateless. Takes the full accumulated bytes, splits on `\n\n` boundaries, extracts `event:` and `data:` fields. Knows the SSE line protocol but nothing about LLM providers.

**Edge cases handled:**
- Multi-line `data:` fields — concatenates all `data:` lines within one event
- `data: [DONE]` — returned as-is in `SSEEvent.Data`; the extractor decides what to do
- BOM / leading whitespace — stripped before parsing
- Empty events (`\n\n` with no fields) — skipped (SSE spec keep-alives)
- Lines without colon — treated as comments per SSE spec, ignored

**Implementation:**

```go
func (p *sseParser) Parse(raw []byte) ([]SSEEvent, error) {
    var events []SSEEvent
    blocks := bytes.Split(raw, []byte("\n\n"))

    for _, block := range blocks {
        block = bytes.TrimSpace(block)
        if len(block) == 0 {
            continue
        }

        var event SSEEvent
        var dataLines [][]byte

        lines := bytes.Split(block, []byte("\n"))
        for _, line := range lines {
            if bytes.HasPrefix(line, []byte("event:")) {
                event.Type = string(bytes.TrimSpace(line[6:]))
            } else if bytes.HasPrefix(line, []byte("data:")) {
                dataLines = append(dataLines, bytes.TrimSpace(line[5:]))
            }
        }

        if len(dataLines) > 0 {
            event.Data = json.RawMessage(bytes.Join(dataLines, []byte("\n")))
        }
        events = append(events, event)
    }
    return events, nil
}
```

#### TokenExtractor — the Strategy interface

```go
type TokenExtractor interface {
    Extract(events []SSEEvent) (TokenUsage, error)
    SupportsFormat(contentType string) bool
}
```

Each provider gets its own implementation. The extractor knows:
- Where to find usage fields in the JSON
- Whether counts are cumulative (take the last value) or deltas (sum them)
- What the stream termination signal is

| Extractor | Semantics | Termination Signal |
|-----------|-----------|-------------------|
| `OpenAIExtractor` | Cumulative — last chunk wins | `data: [DONE]` |
| `AnthropicExtractor` | Delta — sum `output_tokens` across `message_delta` events | `event: message_stop` |
| `GenericExtractor` | Fallback — scan all events for any `usage` field | `data: [DONE]` or stream close |

**OpenAI Extractor** — cumulative semantics:

```go
func (e *OpenAIExtractor) Extract(events []SSEEvent) (TokenUsage, error) {
    var usage TokenUsage

    for _, event := range events {
        if string(event.Data) == "[DONE]" {
            break
        }

        var chunk struct {
            Usage *struct {
                PromptTokens     int64 `json:"prompt_tokens"`
                CompletionTokens int64 `json:"completion_tokens"`
                TotalTokens      int64 `json:"total_tokens"`
            } `json:"usage"`
        }

        if err := json.Unmarshal(event.Data, &chunk); err != nil {
            continue
        }

        if chunk.Usage != nil {
            usage.PromptTokens = chunk.Usage.PromptTokens
            usage.CompletionTokens = chunk.Usage.CompletionTokens
            usage.TotalTokens = chunk.Usage.TotalTokens
        }
    }
    return usage, nil
}
```

**Anthropic Extractor** — delta semantics:

```go
func (e *AnthropicExtractor) Extract(events []SSEEvent) (TokenUsage, error) {
    var usage TokenUsage

    for _, event := range events {
        switch event.Type {
        case "message_start":
            var msg struct {
                Message struct {
                    Usage struct {
                        InputTokens int64 `json:"input_tokens"`
                    } `json:"usage"`
                } `json:"message"`
            }
            if err := json.Unmarshal(event.Data, &msg); err == nil {
                usage.PromptTokens = msg.Message.Usage.InputTokens
            }

        case "message_delta":
            var delta struct {
                Usage struct {
                    OutputTokens int64 `json:"output_tokens"`
                } `json:"usage"`
            }
            if err := json.Unmarshal(event.Data, &delta); err == nil {
                usage.CompletionTokens += delta.Usage.OutputTokens
            }

        case "message_stop":
            break
        }
    }

    usage.TotalTokens = usage.PromptTokens + usage.CompletionTokens
    return usage, nil
}
```

**Why typed structs instead of `map[string]any`?** Rust-inspired type safety. Each extractor unmarshals into a narrow struct containing only the fields it needs. Unknown fields are ignored by `encoding/json`. This avoids runtime type assertion chains like `body["usage"].(map[string]any)["completion_tokens"].(float64)` — no nil pointer panics, no silent type mismatches.

#### TokenAccumulator — composition root

```go
type TokenAccumulator interface {
    Accumulate(raw []byte, contentType string) (TokenUsage, error)
}
```

The default implementation holds an `SSEParser` and a registry of `TokenExtractor` implementations. On `Accumulate()`:

1. Calls `SSEParser.Parse(raw)` to get events
2. Iterates extractors in priority order, calls `SupportsFormat(contentType)` to find the right one (Chain of Responsibility pattern)
3. Delegates to `extractor.Extract(events)`
4. Returns the `TokenUsage`

### Plugin Integration

The streaming token counter is a standard `ResponseProcessor` plugin — no changes to the plugin framework.

#### Plugin struct

```go
const PluginType = "streaming-token-counter"

type StreamingTokenCounterPlugin struct {
    typedName   plugin.TypedName
    accumulator TokenAccumulator
}

func (p *StreamingTokenCounterPlugin) TypedName() plugin.TypedName {
    return p.typedName
}

func (p *StreamingTokenCounterPlugin) ProcessResponse(
    ctx context.Context,
    cycleState *plugin.CycleState,
    response *requesthandling.InferenceResponse,
) error {
    contentType := response.Headers["content-type"]
    if !isSSEContentType(contentType) {
        return nil
    }

    rawBytes, err := plugin.ReadCycleStateKey[[]byte](cycleState, "response_body_raw")
    if err != nil {
        return nil
    }

    usage, err := p.accumulator.Accumulate(rawBytes, contentType)
    if err != nil {
        log.FromContext(ctx).V(logutil.VERBOSE).Error(err, "SSE token extraction failed")
        return nil
    }

    cycleState.Write("token_usage", usage)
    return nil
}
```

#### Handler change — SSE content-type detection

One targeted change in `response.go:HandleResponseBody` — detect SSE content-type before attempting JSON unmarshal:

```go
func (s *Server) HandleResponseBody(ctx context.Context, reqCtx *RequestContext, responseBodyBytes []byte) ([]*eppb.ProcessingResponse, error) {
    // ... existing datalayer event notification ...

    if isSSEContentType(reqCtx.Response.Headers["content-type"]) {
        reqCtx.CycleState.Write("response_body_raw", responseBodyBytes)
        // SSE is not a single JSON document — skip json.Unmarshal
    } else {
        if err := json.Unmarshal(responseBodyBytes, &reqCtx.Response.Body); err != nil {
            // ... existing error handling ...
        }
    }

    // ... existing plugin execution and mutation handling ...
}
```

This keeps the handler change minimal — a single content-type branch. The handler owns transport decisions; the plugin owns SSE semantics.

#### Factory registration

Follows the existing pattern in `cmd/runner/runner.go`:

```go
func registerInTreePlugins() {
    plugin.Register("streaming-token-counter", streamtokencounter.NewPlugin)
    // ... existing registrations
}
```

#### Configuration

```yaml
apiVersion: llm-d.ai/v1alpha1
kind: PayloadProcessorConfig
plugins:
- name: streaming-token-counter
  type: streaming-token-counter
  parameters:
    providers:
    - openai       # enable OpenAI extractor
    - anthropic    # enable Anthropic extractor
    - generic      # fallback extractor
profiles:
- name: default
  plugins:
    response:
    - pluginRef: streaming-token-counter   # must run first
    - pluginRef: token-usage-headers       # reads TokenUsage from CycleState
```

#### Plugin ordering

The streaming token counter must run **first** in the response plugin chain so downstream plugins can read `token_usage` from CycleState. Consumers use the type-safe helper:

```go
usage, err := plugin.ReadCycleStateKey[TokenUsage](cycleState, "token_usage")
```

### Error Handling & Observability

Every failure mode has a named metric and a clear recovery path. Token counting is observability infrastructure — it must never break the response path.

| Failure | Severity | Response | Metric |
|---------|----------|----------|--------|
| Not an SSE response | Expected | Skip plugin silently | None |
| SSE parse failure (malformed bytes) | Warning | Log, return zero counts | `ipp_sse_parse_errors_total` |
| JSON unmarshal failure on one chunk | Warning | Skip that chunk, continue | `ipp_sse_chunk_parse_errors_total` |
| No extractor matches content-type | Warning | Log, return zero counts | `ipp_sse_unknown_format_total` |
| No usage fields found in any chunk | Info | Return zero counts | `ipp_sse_no_usage_total` |
| Successful extraction | Info | Write to CycleState | `ipp_streaming_token_count_total` |

**Prometheus metrics:**

```
ipp_streaming_token_count_total        counter   {model, token_type: prompt|completion}
ipp_streaming_token_plugin_seconds     histogram plugin execution latency
ipp_sse_parse_errors_total             counter   SSE parse failures
ipp_sse_events_parsed_total            counter   {provider} events successfully parsed
```

**Logging:** Uses the existing `go-logr` pattern. Parse failures at `VERBOSE` level (expected for non-standard providers). Successful counts at `DEBUG`.

### Testing Strategy

Each layer is independently testable:

| Layer | Test approach | Input | Assert on |
|-------|--------------|-------|-----------|
| `SSEParser` | Unit test | Raw byte slices (golden files per provider) | `[]SSEEvent` count, types, data content |
| `OpenAIExtractor` | Unit test | `[]SSEEvent` with known usage fields | `TokenUsage` values |
| `AnthropicExtractor` | Unit test | `[]SSEEvent` with delta semantics | Accumulated `TokenUsage` |
| `TokenAccumulator` | Unit test with mocks | Raw bytes + content-type | Correct extractor selected, correct `TokenUsage` |
| `Plugin` | Unit test | `CycleState` with raw bytes, mock accumulator | `TokenUsage` written to `CycleState` |
| End-to-end | Integration test via `Harness` | Full ext_proc stream with SSE response | Token counts in `CycleState`, correct metrics |

**Golden test files:** Real SSE streams captured from OpenAI, Anthropic, and edge cases (empty usage, malformed chunks, keep-alives) stored as test fixtures.

### Package Structure

```
pkg/framework/plugins/requesthandling/streamtokencounter/
├── plugin.go               # ResponseProcessor plugin, factory function
├── plugin_test.go
├── sse/
│   ├── parser.go           # SSEParser implementation
│   ├── parser_test.go
│   └── types.go            # SSEEvent, TokenUsage
├── extractor/
│   ├── extractor.go        # TokenExtractor interface
│   ├── openai.go           # OpenAI cumulative extractor
│   ├── openai_test.go
│   ├── anthropic.go        # Anthropic delta extractor
│   ├── anthropic_test.go
│   ├── generic.go          # Fallback extractor
│   └── generic_test.go
├── accumulator.go          # TokenAccumulator — wires parser + extractors
├── accumulator_test.go
└── testdata/
    ├── openai_stream.txt
    ├── anthropic_stream.txt
    ├── empty_usage.txt
    └── malformed_chunks.txt
```

### Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│  ResponseProcessor Plugin (streaming-token-counter)              │
│  - detects SSE via content-type                                  │
│  - reads raw bytes from CycleState                               │
│  - writes TokenUsage to CycleState                               │
├──────────────────────────────────────────────────────────────────┤
│  TokenAccumulator                                                │
│  - orchestrates parser + extractor                               │
│  - selects extractor via SupportsFormat() (Chain of Resp.)       │
├───────────────────────────┬──────────────────────────────────────┤
│  SSEParser                │  TokenExtractor (Strategy pattern)   │
│  - byte splitting         │  - OpenAIExtractor (cumulative)     │
│  - SSE line protocol      │  - AnthropicExtractor (deltas)      │
│  - zero-copy RawMessage   │  - GenericExtractor (fallback)      │
└───────────────────────────┴──────────────────────────────────────┘
```

### CycleState Data Contract

| Key | Type | Written by | Read by |
|-----|------|-----------|---------|
| `response_body_raw` | `[]byte` | `HandleResponseBody` (handler) | `streaming-token-counter` plugin |
| `token_usage` | `TokenUsage` | `streaming-token-counter` plugin | Downstream response plugins |

### Future Considerations

If mid-stream token counting becomes necessary (e.g., for real-time rate limiting), the architecture can evolve to Approach 1 (incremental) by:

1. Moving `SSEParser` to a stateful, `Feed()`/`Drain()` API
2. Introducing a new `StreamingResponseProcessor` interface with per-chunk hooks
3. Calling the parser inside the recv loop

The `TokenExtractor` strategy implementations and `TokenUsage` types remain unchanged — only the orchestration layer changes. This is why the separation of concerns matters: the provider-specific logic is insulated from the transport-level decision.
