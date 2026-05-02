package bus

// Subject layout:
//
//   {prefix}.events.{event_type}                  // broadcast
//   {prefix}.invoke.{target}                      // request entry
//   {prefix}.{tenant}.events.{event_type}         // tenant-scoped broadcast
//   {prefix}.{tenant}.invoke.{target}             // tenant-scoped request
//
// _INBOX subjects are allocated by the transport (NATS) and embedded in
// envelope.ReplyTo; the bus does not construct them.

func (b *defaultBus) eventSubject(eventType, tenant string) string {
	if tenant != "" {
		return b.opts.SubjectPrefix + "." + tenant + ".events." + eventType
	}
	return b.opts.SubjectPrefix + ".events." + eventType
}

func (b *defaultBus) invokeSubject(target, tenant string) string {
	if tenant != "" {
		return b.opts.SubjectPrefix + "." + tenant + ".invoke." + target
	}
	return b.opts.SubjectPrefix + ".invoke." + target
}

// resolveTenant picks the per-message tenant ID, falling back to the
// bus-level Tenant option.
func (b *defaultBus) resolveTenant(envelopeTenant string) string {
	if envelopeTenant != "" {
		return envelopeTenant
	}
	return b.opts.Tenant
}
