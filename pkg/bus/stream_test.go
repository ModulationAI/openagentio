package bus_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ModulationAI/agentflowbus/pkg/bus"
	"github.com/ModulationAI/agentflowbus/pkg/event"
)

func TestStreamInvokeHappyPath(t *testing.T) {
	b, done := newTestBus(t, "stream-agent")
	defer done()

	if err := b.HandleStream("count", func(_ context.Context, _ *event.Envelope, w bus.StreamWriter) error {
		if err := w.Started(map[string]any{"model": "test"}); err != nil {
			return err
		}
		for i := 0; i < 3; i++ {
			if err := w.Delta(map[string]any{"i": i}); err != nil {
				return err
			}
		}
		return w.Final(map[string]any{"count": 3})
	}); err != nil {
		t.Fatalf("HandleStream: %v", err)
	}

	s, err := b.StreamInvoke(context.Background(), "count", nil)
	if err != nil {
		t.Fatalf("StreamInvoke: %v", err)
	}
	defer s.Close()

	var (
		types []string
		seqs  []uint64
	)
	for env, err := range s.Events() {
		if err != nil {
			t.Fatalf("frame error: %v", err)
		}
		types = append(types, env.EventType)
		seqs = append(seqs, env.Seq)
	}

	wantTypes := []string{
		event.ResponseStarted,
		event.ResponseDelta,
		event.ResponseDelta,
		event.ResponseDelta,
		event.ResponseFinal,
	}
	if len(types) != len(wantTypes) {
		t.Fatalf("types = %v want %v", types, wantTypes)
	}
	for i, ty := range wantTypes {
		if types[i] != ty {
			t.Errorf("types[%d] = %q want %q", i, types[i], ty)
		}
		if seqs[i] != uint64(i) {
			t.Errorf("seqs[%d] = %d want %d", i, seqs[i], i)
		}
	}
}

func TestStreamInvokeAutoFinalOnNilReturn(t *testing.T) {
	b, done := newTestBus(t, "stream-agent")
	defer done()

	if err := b.HandleStream("auto", func(_ context.Context, _ *event.Envelope, w bus.StreamWriter) error {
		_ = w.Started(nil)
		_ = w.Delta(nil)
		return nil // runtime should auto-emit Final
	}); err != nil {
		t.Fatalf("HandleStream: %v", err)
	}

	s, err := b.StreamInvoke(context.Background(), "auto", nil)
	if err != nil {
		t.Fatalf("StreamInvoke: %v", err)
	}
	defer s.Close()

	var last *event.Envelope
	for env, err := range s.Events() {
		if err != nil {
			t.Fatalf("frame error: %v", err)
		}
		last = env
	}
	if last == nil || last.EventType != event.ResponseFinal {
		t.Fatalf("last event = %+v want ResponseFinal", last)
	}
	if !last.IsFinal {
		t.Fatal("last event not is_final")
	}
}

func TestStreamInvokeAutoErrorOnHandlerReturn(t *testing.T) {
	b, done := newTestBus(t, "stream-agent")
	defer done()

	if err := b.HandleStream("explode", func(_ context.Context, _ *event.Envelope, w bus.StreamWriter) error {
		_ = w.Started(nil)
		return errors.New("kaboom")
	}); err != nil {
		t.Fatalf("HandleStream: %v", err)
	}

	s, err := b.StreamInvoke(context.Background(), "explode", nil)
	if err != nil {
		t.Fatalf("StreamInvoke: %v", err)
	}
	defer s.Close()

	var last *event.Envelope
	for env, err := range s.Events() {
		if err != nil {
			t.Fatalf("frame error: %v", err)
		}
		last = env
	}
	if last == nil || last.EventType != event.ResponseError {
		t.Fatalf("last event = %+v want ResponseError", last)
	}

	var p event.ErrorPayload
	if err := json.Unmarshal(last.Payload, &p); err != nil {
		t.Fatalf("decode error payload: %v", err)
	}
	if p.Message != "kaboom" {
		t.Fatalf("error message = %q", p.Message)
	}
}

func TestStreamInvokeIdleTimeout(t *testing.T) {
	b, done := newTestBus(t, "stream-agent")
	defer done()

	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })

	if err := b.HandleStream("hang", func(ctx context.Context, _ *event.Envelope, w bus.StreamWriter) error {
		_ = w.Started(nil)
		// Park here longer than the client's idle timeout.
		select {
		case <-hold:
		case <-ctx.Done():
		}
		return nil
	}); err != nil {
		t.Fatalf("HandleStream: %v", err)
	}

	s, err := b.StreamInvoke(context.Background(), "hang", nil, bus.WithIdleTimeout(50*time.Millisecond))
	if err != nil {
		t.Fatalf("StreamInvoke: %v", err)
	}
	defer s.Close()

	var lastErr error
	gotStart := false
	for env, err := range s.Events() {
		if err != nil {
			lastErr = err
			break
		}
		if env.EventType == event.ResponseStarted {
			gotStart = true
		}
	}
	if !gotStart {
		t.Fatal("expected ResponseStarted before idle timeout")
	}
	if !errors.Is(lastErr, bus.ErrIdleTimeout) {
		t.Fatalf("got err %v want ErrIdleTimeout", lastErr)
	}
}

func TestStreamInvokeCloseStopsIteration(t *testing.T) {
	b, done := newTestBus(t, "stream-agent")
	defer done()

	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })

	if err := b.HandleStream("park", func(ctx context.Context, _ *event.Envelope, w bus.StreamWriter) error {
		_ = w.Started(nil)
		select {
		case <-hold:
		case <-ctx.Done():
		}
		return nil
	}); err != nil {
		t.Fatalf("HandleStream: %v", err)
	}

	s, err := b.StreamInvoke(context.Background(), "park", nil)
	if err != nil {
		t.Fatalf("StreamInvoke: %v", err)
	}

	gotFirst := make(chan struct{})
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		first := true
		for _, err := range s.Events() {
			if first {
				close(gotFirst)
				first = false
			}
			_ = err
		}
	}()

	<-gotFirst
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("iteration did not exit after Close")
	}
}

func TestStreamWriterStartedAtMostOnce(t *testing.T) {
	b, done := newTestBus(t, "stream-agent")
	defer done()

	var (
		secondStartErr error
		gotSecond      = make(chan struct{})
	)
	if err := b.HandleStream("twostart", func(_ context.Context, _ *event.Envelope, w bus.StreamWriter) error {
		_ = w.Started(nil)
		secondStartErr = w.Started(nil)
		close(gotSecond)
		return w.Final(nil)
	}); err != nil {
		t.Fatalf("HandleStream: %v", err)
	}

	s, err := b.StreamInvoke(context.Background(), "twostart", nil)
	if err != nil {
		t.Fatalf("StreamInvoke: %v", err)
	}
	defer s.Close()

	for _, err := range s.Events() {
		if err != nil {
			t.Fatalf("frame error: %v", err)
		}
	}
	<-gotSecond
	if secondStartErr == nil {
		t.Fatal("second Started should error")
	}
}

func TestStreamWriterFinalIsTerminal(t *testing.T) {
	b, done := newTestBus(t, "stream-agent")
	defer done()

	var (
		afterFinalErr error
		captured      = make(chan struct{})
	)
	if err := b.HandleStream("late", func(_ context.Context, _ *event.Envelope, w bus.StreamWriter) error {
		if err := w.Final(nil); err != nil {
			return err
		}
		afterFinalErr = w.Delta(nil)
		close(captured)
		return nil
	}); err != nil {
		t.Fatalf("HandleStream: %v", err)
	}

	s, err := b.StreamInvoke(context.Background(), "late", nil)
	if err != nil {
		t.Fatalf("StreamInvoke: %v", err)
	}
	defer s.Close()

	var count int
	for _, err := range s.Events() {
		if err != nil {
			t.Fatalf("frame error: %v", err)
		}
		count++
	}
	<-captured
	if afterFinalErr == nil {
		t.Fatal("Delta after Final should error")
	}
	if count != 1 {
		t.Fatalf("frame count = %d want 1", count)
	}
}
