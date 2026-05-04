<div align="center">
<p align="center">
<img src="https://cdn.jsdelivr.net/gh/ModulationAI/agentflowbus/assets/logo.png?raw=true" width="600" height="200" alt="agentflowbus preview">
</p>
</div>


# AgentFlowBus

AgentFlowBus is a lightweight communication runtime for AI agents. It provides an ACP-compatible event envelope, streaming-first request/reply APIs, session and trace propagation, and pluggable transports such as in-memory and NATS Core.

The project focuses on agent communication infrastructure. It does not implement planning, RAG, prompt management, tool execution, or agent orchestration.

## Why

AI agent systems often need more than plain HTTP calls:

- streaming responses for LLM token output;
- multiple agents or workers consuming the same task stream;
- consistent session, conversation, tenant, and trace metadata;
- request/reply and pub/sub in the same protocol;
- a transport-neutral contract that can be shared across Go, Python, and TypeScript SDKs.

AgentFlowBus addresses that layer with a small Go runtime and a protocol-first design.

## Status

This repository is currently at **v0.2**.

Implemented:

- `Envelope` protocol model with schema samples;
- JSON codec;
- `Publish` / `Subscribe`;
- `Invoke` / `HandleInvoke`;
- `StreamInvoke` / `HandleStream`;
- in-memory transport for tests and local examples;
- NATS Core transport for publish, subscribe, queue groups, request/reply, and inbox streams;
- recover, trace, structured logging, **retry**, and **dead-letter** middleware;
- **OpenTelemetry** bridge (`pkg/middleware/otel`) — opt-in, no hard dependency;
- **HTTP/SSE adapter** (`pkg/adapter/http`) for driving the bus over REST;
- **Python SDK** (`sdk/python/`) with asyncio bus, session context propagation, and stream invoke.

Not yet implemented:

- JetStream persistence, ack, replay, and native DLQ;
- TypeScript SDK;
- auth middleware;
- metrics and dashboards.

For the original goals and design rationale, see [`prompts/require.md`](prompts/require.md), [`prompts/design.md`](prompts/design.md), and the code report in [`prompts/codex_0.1_report.md`](prompts/codex_0.1_report.md).

## Project Layout

```text
pkg/
├── event/         # Envelope, event types, payloads, UUIDv7 IDs
├── codec/         # Codec interface and JSON implementation
├── transport/     # Transport abstraction
│   ├── inmem/     # In-memory broker for tests and examples
│   └── nats/      # NATS Core driver
├── bus/           # Public Bus API and runtime implementation
├── middleware/    # Recover, Trace, Logging, Retry, DeadLetter
│   └── otel/      # OpenTelemetry bridge (opt-in dependency)
├── session/       # Context helpers for trace/session metadata
└── adapter/http/  # HTTP/SSE gateway

sdk/python/        # Python asyncio SDK
schema/            # JSON Schema and cross-language envelope samples
examples/
├── echo-agent/    # Minimal invoke round-trip example
├── http-gateway/  # HTTP/SSE adapter example
└── streaming-llm/ # StreamInvoke / HandleStream example
prompts/           # Requirements, design notes, and code reports
```

## Install

```sh
go get github.com/ModulationAI/agentflowbus
```

The module pins `go 1.25` and `toolchain go1.25.0` in `go.mod`.

## Quick Start

```go
package main

import (
	"context"
	"fmt"

	"github.com/ModulationAI/agentflowbus/pkg/bus"
	"github.com/ModulationAI/agentflowbus/pkg/event"
	"github.com/ModulationAI/agentflowbus/pkg/transport/inmem"
)

func main() {
	b, err := bus.New(
		bus.WithAgentID("echo-agent"),
		bus.WithTransport(inmem.New()),
	)
	if err != nil {
		panic(err)
	}
	defer b.Close()

	err = b.HandleInvoke("echo", func(_ context.Context, e *event.Envelope) (any, error) {
		return map[string]any{"echo": string(e.Payload)}, nil
	})
	if err != nil {
		panic(err)
	}

	resp, err := b.Invoke(context.Background(), "echo", map[string]any{"msg": "hello"})
	if err != nil {
		panic(err)
	}

	fmt.Println(resp.EventType, string(resp.Payload))
}
```

Run the bundled examples:

```sh
go run ./examples/echo-agent      # simple request/reply
go run ./examples/streaming-llm   # stream invoke with delta frames
go run ./examples/http-gateway    # HTTP/SSE adapter on :8080
```

### Streaming Quick Start

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/ModulationAI/agentflowbus/pkg/bus"
    "github.com/ModulationAI/agentflowbus/pkg/event"
    "github.com/ModulationAI/agentflowbus/pkg/transport/inmem"
)

func main() {
    b, _ := bus.New(
        bus.WithAgentID("stream-agent"),
        bus.WithTransport(inmem.New()),
    )
    defer b.Close()

    _ = b.HandleStream("chat", func(ctx context.Context, e *event.Envelope, w bus.StreamWriter) error {
        _ = w.Started(nil)
        _ = w.Delta(map[string]string{"token": "hello "})
        _ = w.Delta(map[string]string{"token": "world"})
        return w.Final(map[string]string{"done": "true"})
    })

    stream, _ := b.StreamInvoke(context.Background(), "chat", nil,
        bus.WithTimeout(30*time.Second),
    )
    defer stream.Close()

    for env, err := range stream.Events() {
        if err != nil {
            panic(err)
        }
        fmt.Println(env.EventType, string(env.Payload))
    }
}
```

## Core Concepts

### Envelope

Every message is carried in an `event.Envelope`. The envelope stores protocol version, event type, IDs, trace/session metadata, tenant information, reply routing, sequence numbers, and an opaque JSON payload.

### Subject Routing

AgentFlowBus separates event semantics from transport routing.

```text
acp.v1.events.{event_type}
acp.v1.invoke.{target}
acp.v1.{tenant}.events.{event_type}
acp.v1.{tenant}.invoke.{target}
```

`event_type` describes what happened. The subject determines where the message goes.

### Streaming

`StreamInvoke` opens an inbox, sends a request with `reply_to`, and returns a stream of response envelopes. Server handlers use `StreamWriter` to emit:

```text
agent.response.started
agent.response.delta
agent.response.delta
agent.response.final
```

Each frame receives a monotonically increasing `seq`. The client stream reorders frames by `seq` and stops when `is_final=true`.

### Middleware

Middleware wraps handler invocations with cross-cutting concerns. The recommended order is outer-most first:

```go
b, _ := bus.New(
    bus.WithAgentID("agent"),
    bus.WithTransport(inmem.New()),
    bus.WithMiddleware(
        middleware.Recover(),
        middleware.Trace(),
        middleware.Logging(logger),
        middleware.Retry(middleware.RetryPolicy{MaxAttempts: 3}),
    ),
)
```

- **Recover** — catches panics, converts them to errors, and logs stack traces.
- **Trace** — injects envelope trace/session metadata into `context`.
- **Logging** — emits a structured log line per invocation with duration.
- **Retry** — retries failed invocations with configurable backoff.
- **DeadLetter** — forwards exhausted failures to a DLQ sink.

For OpenTelemetry integration, import `pkg/middleware/otel` and register `otel.Trace()` middleware plus `otel.EnvelopePreparer()` as a `bus.WithEnvelopePreparer` option.

## Development Commands

```sh
go mod tidy      # update module dependencies
go build ./...   # compile all packages and examples
go test ./...    # run unit and golden tests
go test ./... -race
```

Focused tests:

```sh
go test ./pkg/bus -run TestStreamInvokeHappyPath
go test ./pkg/event
```

## NATS Usage

Use the NATS transport when running across processes:

```go
import nats "github.com/ModulationAI/agentflowbus/pkg/transport/nats"

tr := nats.New(
	nats.URL("nats://localhost:4222"),
	nats.Name("agentflowbus-main"),
)

b, err := bus.New(
	bus.WithAgentID("main-agent"),
	bus.WithTransport(tr),
)
```

NATS Core support is available today. Durable delivery and replay are planned for a future JetStream transport.

## Python SDK

A Python asyncio SDK lives in `sdk/python/`:

```python
import asyncio
from agentflowbus import Bus, InMemoryDriver

async def main():
    bus = Bus(agent_id="echo", transport=InMemoryDriver())
    await bus.connect()

    async def echo(env):
        return env.payload_json()

    await bus.handle_invoke("echo", echo)
    resp = await bus.invoke("echo", {"msg": "hello"})
    print(resp.event_type, resp.payload_json())
    await bus.close()

asyncio.run(main())
```

Install it with `pip install -e sdk/python/` (the package name is `agentflowbus`).

## Roadmap

- v0.1: Go runtime, envelope schema, in-memory transport, NATS Core transport, invoke and streaming APIs.
- v0.2: HTTP/SSE adapter, Python SDK, session/trace propagation, OpenTelemetry bridge, retry / dead-letter middleware.
- v0.3: JetStream persistence and replay, auth middleware, metrics, TypeScript SDK.
- v1.0: stable cross-language protocol and production deployment guidance.

## License

License information has not been added yet.
