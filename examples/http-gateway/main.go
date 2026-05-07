// http-gateway boots an OpenAgentIO + HTTP/SSE adapter on :8080 so external
// clients can drive the bus over REST and SSE. Two handlers are registered
// against the in-memory transport so the demo is self-contained:
//
//   - echo  : POST /v1/agents/echo/invoke   returns the request payload as-is.
//   - count : POST /v1/agents/count/stream  emits started + N deltas + final.
//
// Smoke test (in another terminal):
//
//	curl -sS -X POST localhost:8080/v1/agents/echo/invoke \
//	     -H 'Content-Type: application/json' \
//	     -d '{"msg":"hi"}'
//
//	curl -sN -X POST localhost:8080/v1/agents/count/stream \
//	     -H 'Content-Type: application/json' \
//	     -d '{"n":3}'
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	nethttp "net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	httpadapter "github.com/ModulationAI/openagentio/pkg/adapter/http"
	"github.com/ModulationAI/openagentio/pkg/bus"
	"github.com/ModulationAI/openagentio/pkg/event"
	"github.com/ModulationAI/openagentio/pkg/middleware"
	"github.com/ModulationAI/openagentio/pkg/transport/inmem"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	b, err := bus.New(
		bus.WithAgentID("http-gateway"),
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
		fmt.Fprintf(os.Stderr, "register echo: %v\n", err)
		os.Exit(1)
	}

	if err := b.HandleStream("count", func(ctx context.Context, e *event.Envelope, w bus.StreamWriter) error {
		var args struct {
			N int `json:"n"`
		}
		if len(e.Payload) > 0 {
			_ = json.Unmarshal(e.Payload, &args)
		}
		if args.N <= 0 {
			args.N = 5
		}
		if err := w.Started(map[string]any{"n": args.N}); err != nil {
			return err
		}
		for i := 0; i < args.N; i++ {
			if err := w.Delta(map[string]int{"i": i}); err != nil {
				return err
			}
			select {
			case <-time.After(150 * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return w.Final(map[string]int{"total": args.N})
	}); err != nil {
		fmt.Fprintf(os.Stderr, "register count: %v\n", err)
		os.Exit(1)
	}

	adapter := httpadapter.New(b,
		httpadapter.WithLogger(logger),
		httpadapter.WithTimeout(30*time.Second),
		httpadapter.WithIdleTimeout(10*time.Second),
		httpadapter.WithMiddleware(
			httpadapter.Recover(logger),
			httpadapter.Logging(logger),
		),
	)

	addr := ":8080"
	if v := os.Getenv("ADDR"); v != "" {
		addr = v
	}
	srv := &nethttp.Server{
		Addr:              addr,
		Handler:           adapter,
		ReadHeaderTimeout: 5 * time.Second,
	}

	idleConnsClosed := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		logger.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		close(idleConnsClosed)
	}()

	logger.Info("http-gateway listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && err != nethttp.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "listen: %v\n", err)
		os.Exit(1)
	}
	<-idleConnsClosed
}
