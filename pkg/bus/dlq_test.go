package bus

import (
	"context"
	"errors"
	"testing"

	"github.com/ModulationAI/agentflowbus/pkg/codec"
	"github.com/ModulationAI/agentflowbus/pkg/event"
	"github.com/ModulationAI/agentflowbus/pkg/transport"
	"github.com/ModulationAI/agentflowbus/pkg/transport/inmem"
)

func TestDLQSinkPublishesToCorrectSubject(t *testing.T) {
	driver := inmem.New()
	cd := codec.JSON()

	var captured *event.Envelope
	_, err := driver.Subscribe(context.Background(), "acp.v1.dlq.foo", "", func(_ context.Context, msg *transport.RawMessage) error {
		env, err := cd.DecodeEnvelope(msg.Data)
		if err != nil {
			return err
		}
		captured = env
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	sink := DLQSink("acp.v1", cd, driver)
	e := event.New("foo")
	if err := sink(context.Background(), e, errors.New("boom")); err != nil {
		t.Fatalf("unexpected sink error: %v", err)
	}

	if captured == nil {
		t.Fatal("expected DLQ message to be published")
	}
	if captured.EventType != "foo" {
		t.Fatalf("expected event_type foo, got %s", captured.EventType)
	}
}

func TestDLQSinkStampsMetadata(t *testing.T) {
	driver := inmem.New()
	cd := codec.JSON()

	var captured *event.Envelope
	_, err := driver.Subscribe(context.Background(), "acp.v1.dlq.bar", "", func(_ context.Context, msg *transport.RawMessage) error {
		env, err := cd.DecodeEnvelope(msg.Data)
		if err != nil {
			return err
		}
		captured = env
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	sink := DLQSink("acp.v1", cd, driver)
	e := event.New("bar")
	if err := sink(context.Background(), e, errors.New("something broke")); err != nil {
		t.Fatalf("unexpected sink error: %v", err)
	}

	if captured == nil {
		t.Fatal("expected DLQ message")
	}
	if captured.Metadata["acp.dlq.original_event_type"] != "bar" {
		t.Fatalf("unexpected original_event_type: %v", captured.Metadata["acp.dlq.original_event_type"])
	}
	if captured.Metadata["acp.dlq.last_error"] != "something broke" {
		t.Fatalf("unexpected last_error: %v", captured.Metadata["acp.dlq.last_error"])
	}
}

func TestDLQSinkOmitsLastErrorWhenNil(t *testing.T) {
	driver := inmem.New()
	cd := codec.JSON()

	var captured *event.Envelope
	_, err := driver.Subscribe(context.Background(), "acp.v1.dlq.baz", "", func(_ context.Context, msg *transport.RawMessage) error {
		env, err := cd.DecodeEnvelope(msg.Data)
		if err != nil {
			return err
		}
		captured = env
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	sink := DLQSink("acp.v1", cd, driver)
	e := event.New("baz")
	if err := sink(context.Background(), e, nil); err != nil {
		t.Fatalf("unexpected sink error: %v", err)
	}

	if captured == nil {
		t.Fatal("expected DLQ message")
	}
	if _, ok := captured.Metadata["acp.dlq.last_error"]; ok {
		t.Fatal("expected last_error to be omitted when nil")
	}
	if captured.Metadata["acp.dlq.original_event_type"] != "baz" {
		t.Fatalf("unexpected original_event_type: %v", captured.Metadata["acp.dlq.original_event_type"])
	}
}
