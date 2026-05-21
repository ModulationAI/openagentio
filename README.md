<div align="center">
<p align="center">
<img src="https://raw.githubusercontent.com/ModulationAI/openagentio/refs/heads/main/assets/OpenAgentIO.png" width="700" height="350" alt="openagentio preview">

<h3 align="center">
OpenAgentIO
</h3>
<h5 align="center">
A protocol & runtime for agent-to-agent communication
</h5>
</div>

OpenAgentIO is a lightweight communication runtime for AI agents. It provides an ACP-compatible event envelope, streaming-first request/reply APIs, session and trace propagation, and pluggable transports such as in-memory and NATS Core.

The project focuses on agent communication infrastructure. It does not implement planning, RAG, prompt management, tool execution, or agent orchestration.

## Why

 <img src="https://github.com/ModulationAI/openagentio/blob/main/assets/show.png?raw=true" alt="openagentio show">

AI agent systems often need more than plain HTTP calls:

- streaming responses for LLM token output;
- multiple agents or workers consuming the same task stream;
- consistent session, conversation, tenant, and trace metadata;
- request/reply and pub/sub in the same protocol;
- a transport-neutral contract that can be shared across Go, Python, and TypeScript SDKs.

OpenAgentIO addresses that layer with a small Go runtime and a protocol-first design.

## Problem Solved

Distributed Agent Communication Complexity

| Category | OpenAgentIO |
|---|---|
| Positioning | Agent Runtime Bus |
| Focus | Agent-to-Agent Communication |
| Protocols | invoke / stream / pubsub |
| Solves | Distributed A2A Runtime Communication |
| Architecture Layer | East-West Communication |
| Core Capabilities | Context / Session / Streaming |
| Envelope Model | Unified Envelope-Based Messaging |
| Context Propagation | Trace / Session Propagation |
| Runtime Support | Cross-Runtime Communication |
| Typical Scenarios | Multi-Agent Runtime |

## Communication Scenarios Coverage

| Scenario | Communication Pattern |
| --- | --- |
| ⭐️ Request-Reply | Synchronous Invocation |
| ⭐️ Streaming | Streaming Response |
| ⭐️ Pub/Sub | Event-Driven Messaging |
| ⭐️ Parallel Execution | Parallel Invocation |
| ⭐️ Agent Handoff | Context Transfer |
| ⭐️ Async Task | Asynchronous Task Processing |


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

1. Go SDK (1.25+)
```sh
go get github.com/ModulationAI/openagentio
```

2. Python SDK (3.10+)
```sh
pip install openagentio
```


## Roadmap

- v0.1: Go runtime, envelope schema, in-memory transport, NATS Core transport, invoke and streaming APIs.
- v0.2: HTTP/SSE adapter, Python SDK, session/trace propagation, OpenTelemetry bridge, retry / dead-letter middleware.
- v0.3: JetStream persistence and replay, auth middleware, metrics, TypeScript SDK.
- v1.0: stable cross-language protocol and production deployment guidance.

## License

License information has not been added yet.
