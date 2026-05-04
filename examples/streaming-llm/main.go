// streaming-llm demonstrates a server-side stream handler that simulates an
// LLM producing tokens, and a client that consumes the stream frame by frame.
//
// It wires the v0.2 middleware stack (Recover, Trace, Logging, Retry) and
// uses the in-memory transport so the demo is self-contained.
//
// Run:
//
//	go run ./examples/streaming-llm
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/ModulationAI/agentflowbus/pkg/bus"
	"github.com/ModulationAI/agentflowbus/pkg/codec"
	"github.com/ModulationAI/agentflowbus/pkg/event"
	"github.com/ModulationAI/agentflowbus/pkg/middleware"
	"github.com/ModulationAI/agentflowbus/pkg/transport/inmem"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	b, err := bus.New(
		bus.WithAgentID("llm-agent"),
		bus.WithTransport(inmem.New()),
		bus.WithLogger(logger),
		bus.WithMiddleware(
			middleware.Recover(),
			middleware.Trace(),
			middleware.Logging(logger),
			middleware.Retry(middleware.RetryPolicy{
				MaxAttempts: 3,
				Backoff:     middleware.ConstantBackoff(50 * time.Millisecond),
			}),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bus init failed: %v\n", err)
		os.Exit(1)
	}
	defer b.Close()

	// Register a stream handler that pretends to be an LLM.
	if err := b.HandleStream("llm", func(ctx context.Context, e *event.Envelope, w bus.StreamWriter) error {
		var req struct {
			Prompt string `json:"prompt"`
		}
		if len(e.Payload) > 0 {
			_ = codec.JSON().DecodePayload(e.Payload, &req)
		}
		if req.Prompt == "" {
			req.Prompt = "Hello, world!"
		}

		// Emit the "started" frame with model metadata.
		if err := w.Started(map[string]any{"model": "fake-llm-v1", "prompt": req.Prompt}); err != nil {
			return err
		}

		// Simulate token-by-token streaming.
		tokens := strings.Split("The quick brown fox jumps over the lazy dog .", " ")
		for _, tok := range tokens {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := w.Delta(map[string]string{"token": tok + " "}); err != nil {
				return err
			}
			time.Sleep(80 * time.Millisecond)
		}

		// Final frame carries the consolidated result.
		return w.Final(map[string]any{
			"text":  strings.Join(tokens, " ") + " ",
			"usage": map[string]int{"tokens": len(tokens)},
		})
	}); err != nil {
		fmt.Fprintf(os.Stderr, "register llm handler: %v\n", err)
		os.Exit(1)
	}

	// Client side: stream-invoke the LLM.
	ctx := context.Background()
	stream, err := b.StreamInvoke(ctx, "llm", map[string]any{"prompt": "Tell me a story"},
		bus.WithTimeout(30*time.Second),
		bus.WithIdleTimeout(2*time.Second),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "stream invoke failed: %v\n", err)
		os.Exit(1)
	}
	defer stream.Close()

	fmt.Println("--- stream begins ---")
	for env, err := range stream.Events() {
		if err != nil {
			fmt.Fprintf(os.Stderr, "stream error: %v\n", err)
			os.Exit(1)
		}
		switch env.EventType {
		case event.ResponseStarted:
			fmt.Printf("[started] %s\n", string(env.Payload))
		case event.ResponseDelta:
			fmt.Printf("[delta]   %s", string(env.Payload))
		case event.ResponseFinal:
			fmt.Printf("\n[final]   %s\n", string(env.Payload))
		default:
			fmt.Printf("[%s] %s\n", env.EventType, string(env.Payload))
		}
	}
	fmt.Println("--- stream ends ---")
}
