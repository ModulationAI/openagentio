"""Subject layout helpers. Mirrors pkg/bus/subjects.go.

Subject schema::

  {prefix}.events.{event_type}                   # broadcast
  {prefix}.invoke.{target}                       # request entry
  {prefix}.{tenant}.events.{event_type}          # tenant-scoped broadcast
  {prefix}.{tenant}.invoke.{target}              # tenant-scoped request

_INBOX subjects are allocated by the transport driver and embedded in
``Envelope.reply_to``; the bus does not construct them.
"""

DEFAULT_SUBJECT_PREFIX = "acp.v1"


def event_subject(prefix: str, event_type: str, tenant: str = "") -> str:
    if tenant:
        return f"{prefix}.{tenant}.events.{event_type}"
    return f"{prefix}.events.{event_type}"


def invoke_subject(prefix: str, target: str, tenant: str = "") -> str:
    if tenant:
        return f"{prefix}.{tenant}.invoke.{target}"
    return f"{prefix}.invoke.{target}"
