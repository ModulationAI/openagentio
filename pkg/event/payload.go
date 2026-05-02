package event

// StartedPayload accompanies agent.response.started and carries optional
// upstream metadata (model name, planned tool calls, etc.).
type StartedPayload struct {
	Meta map[string]any `json:"meta,omitempty"`
}

// DeltaPayload is the streamed increment for agent.response.delta. The Delta
// field carries the textual token stream; structured payloads can use Data.
type DeltaPayload struct {
	Delta string         `json:"delta,omitempty"`
	Data  map[string]any `json:"data,omitempty"`
}

// FinalPayload terminates a streaming response with the consolidated result.
type FinalPayload struct {
	Result map[string]any `json:"result,omitempty"`
}

// Standard error codes used by ErrorPayload.Code.
const (
	CodeAgentTimeout      = "AGENT_TIMEOUT"
	CodeAgentUnavailable  = "AGENT_UNAVAILABLE"
	CodeBackpressureDrop  = "BACKPRESSURE_DROP"
	CodeTransportFailure  = "TRANSPORT_FAILURE"
	CodeCodecFailure      = "CODEC_FAILURE"
	CodeAuthFailure       = "AUTH_FAILURE"
	CodeInvalidRequest    = "INVALID_REQUEST"
	CodeNoHandler         = "NO_HANDLER"
)

// ErrorPayload is the standardized failure shape for agent.response.error and
// failed agent.tool.result events.
type ErrorPayload struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	Retryable bool           `json:"retryable"`
	Cause     map[string]any `json:"cause,omitempty"`
}
