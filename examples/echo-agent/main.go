// echo-agent is the minimal AgentFlowBus example: it constructs a Bus backed
// by the in-memory transport, registers an echo invoke handler, and
// round-trips a single Invoke call against itself to demonstrate the wiring.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/ModulationAI/agentflowbus/pkg/bus"
	"github.com/ModulationAI/agentflowbus/pkg/event"
	"github.com/ModulationAI/agentflowbus/pkg/middleware"
	"github.com/ModulationAI/agentflowbus/pkg/transport/inmem"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	b, err := bus.New(
		bus.WithAgentID("echo"),
		bus.WithTransport(inmem.New()),
		bus.WithLogger(logger),
		bus.WithMiddleware(
			middleware.Recover(),
			middleware.Trace(),
			middleware.Logging(logger),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bus init failed: %v\n", err)
		os.Exit(1)
	}
	defer b.Close()

	if err := b.HandleInvoke("echo", func(_ context.Context, e *event.Envelope) (any, error) {
		return json.RawMessage(e.Payload), nil
	}); err != nil {
		fmt.Fprintf(os.Stderr, "handle invoke failed: %v\n", err)
		os.Exit(1)
	}

	resp, err := b.Invoke(context.Background(), "echo", map[string]any{"msg": "hello bus"})
	if err != nil {
		fmt.Fprintf(os.Stderr, "invoke failed: %v\n", err)
		os.Exit(1)
	}

	logger.Info("echo round-trip ok",
		"event_type", resp.EventType,
		"correlation_id", resp.CorrelationID,
		"is_final", resp.IsFinal,
		"payload", string(resp.Payload),
	)
}
