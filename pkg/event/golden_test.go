package event_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v5"

	"github.com/ModulationAI/agentflowbus/pkg/codec"
	"github.com/ModulationAI/agentflowbus/pkg/event"
)

// schemaDir resolves to the repository's schema/ directory regardless of where
// `go test` is invoked from, by anchoring on this test file's location.
func schemaDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "schema")
}

func loadJSON(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}

func compileSchema(t *testing.T, dir string) *jsonschema.Schema {
	t.Helper()
	schemaPath := filepath.Join(dir, "envelope.schema.json")
	sch, err := jsonschema.Compile(schemaPath)
	if err != nil {
		t.Fatalf("compile %s: %v", schemaPath, err)
	}
	return sch
}

// goldenSamples returns the list of sample filenames under schema/samples/.
// Update this when adding a new event-type sample.
func goldenSamples() []string {
	return []string{
		"message_received.json",
		"response_started.json",
		"response_delta.json",
		"response_final.json",
		"response_error.json",
	}
}

func TestSamplesValidateAgainstSchema(t *testing.T) {
	dir := schemaDir(t)
	sch := compileSchema(t, dir)

	for _, name := range goldenSamples() {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(dir, "samples", name)
			data := loadJSON(t, path)

			var v any
			if err := json.Unmarshal(data, &v); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if err := sch.Validate(v); err != nil {
				t.Fatalf("schema validation failed: %#v", err)
			}
		})
	}
}

// TestEnvelopeRoundTripPreservesSamples asserts that each golden sample:
//
//  1. decodes into event.Envelope without leaving any unknown wire fields
//     (DisallowUnknownFields catches sample/struct drift), and
//  2. is fixed-point under the codec — encode(decode(encode(x))) == encode(x).
//
// We compare encoded forms (rather than struct fields or original bytes) so
// the test is robust against two protocol-legal differences: omitempty
// (an explicit `"seq": 0` in a sample becomes an absent field after Go
// re-encode) and payload whitespace (json.RawMessage preserves formatting on
// decode but encoding compacts it).
func TestEnvelopeRoundTripPreservesSamples(t *testing.T) {
	dir := schemaDir(t)
	c := codec.JSON()

	for _, name := range goldenSamples() {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(dir, "samples", name)
			original := loadJSON(t, path)

			dec := json.NewDecoder(bytes.NewReader(original))
			dec.DisallowUnknownFields()
			var first event.Envelope
			if err := dec.Decode(&first); err != nil {
				t.Fatalf("strict decode (sample has field unknown to Envelope?): %v", err)
			}

			encoded1, err := c.EncodeEnvelope(&first)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			second, err := c.DecodeEnvelope(encoded1)
			if err != nil {
				t.Fatalf("re-decode: %v", err)
			}
			encoded2, err := c.EncodeEnvelope(second)
			if err != nil {
				t.Fatalf("re-encode: %v", err)
			}

			if !bytes.Equal(encoded1, encoded2) {
				t.Fatalf("round-trip drift\nencoded1: %s\nencoded2: %s", encoded1, encoded2)
			}
		})
	}
}

func TestEnvelopeRequiredFields(t *testing.T) {
	dir := schemaDir(t)
	sch := compileSchema(t, dir)
	c := codec.JSON()

	env := event.New(event.MessageReceived)
	if env.SpecVersion == "" || env.SchemaVersion == 0 || env.EventID == "" || env.OccurredAt.IsZero() {
		t.Fatalf("event.New produced incomplete envelope: %+v", env)
	}

	encoded, err := c.EncodeEnvelope(env)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var v any
	if err := json.Unmarshal(encoded, &v); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := sch.Validate(v); err != nil {
		t.Fatalf("freshly minted envelope failed schema: %#v", err)
	}
}
