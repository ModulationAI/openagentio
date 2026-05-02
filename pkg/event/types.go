package event

// Spec/schema versions advertised by every Envelope produced by this SDK.
const (
	SpecVersion   = "acp/1.0"
	SchemaVersion = 1
)

// Standard event types.
const (
	// User input.
	MessageReceived = "agent.message.received"

	// Response lifecycle.
	ResponseStarted = "agent.response.started"
	ResponseDelta   = "agent.response.delta"
	ResponseFinal   = "agent.response.final"
	ResponseError   = "agent.response.error"

	// Tool calls (reserved, enabled in v0.2+).
	ToolCall   = "agent.tool.call"
	ToolResult = "agent.tool.result"

	// Async tasks (reserved, enabled in v0.3+ with JetStream).
	TaskCreated   = "agent.task.created"
	TaskCompleted = "agent.task.completed"
)

// IsTerminal reports whether the event type closes a streaming response, i.e.
// no further deltas are expected after it on the same correlation_id.
func IsTerminal(eventType string) bool {
	switch eventType {
	case ResponseFinal, ResponseError, ToolResult, TaskCompleted:
		return true
	default:
		return false
	}
}
