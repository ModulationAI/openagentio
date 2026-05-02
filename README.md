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

This repository is currently at **v0.1**.

Implemented:

- `Envelope` protocol model with schema samples;
- JSON codec;
- `Publish` / `Subscribe`;
- `Invoke` / `HandleInvoke`;
- `StreamInvoke` / `HandleStream`;
- in-memory transport for tests and local examples;
- NATS Core transport for publish, subscribe, queue groups, request/reply, and inbox streams;
- recover, trace, and structured logging middleware.

Not yet implemented:

- JetStream persistence, ack, replay, and DLQ;
- HTTP/SSE adapter;
- Python and TypeScript SDKs;
- OpenTelemetry integration;
- production-grade retry and auth middleware.

For the original goals and design rationale, see [`prompts/require.md`](prompts/require.md), [`prompts/design.md`](prompts/design.md), and the current code report in [`prompts/codex_0.1_report.md`](prompts/codex_0.1_report.md).

## Project Layout

```text
pkg/
├── event/         # Envelope, event types, payloads, UUIDv7 IDs
├── codec/         # Codec interface and JSON implementation
├── transport/     # Transport abstraction
│   ├── inmem/     # In-memory broker for tests and examples
│   └── nats/      # NATS Core driver
├── bus/           # Public Bus API and runtime implementation
├── middleware/    # Recover, Trace, Logging
└── session/       # Context helpers for trace/session metadata

schema/            # JSON Schema and cross-language envelope samples
examples/
└── echo-agent/    # Minimal invoke round-trip example
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

Run the bundled example:

```sh
go run ./examples/echo-agent
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

## Roadmap

- v0.1: Go runtime, envelope schema, in-memory transport, NATS Core transport, invoke and streaming APIs.
- v0.2: HTTP/SSE adapter, stronger error mapping, Python agent SDK, trace improvements.
- v0.3: JetStream reliability, retry hooks, metrics, OpenTelemetry integration.
- v1.0: stable cross-language protocol and production deployment guidance.

## License

License information has not been added yet.
