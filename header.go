package ebus

import (
	"strconv"
)

const (
	HeaderSchemaVersion = "x-event-schema-version"
	HeaderEventId       = "x-event-id"
	HeaderEventSource   = "x-event-source"
	HeaderEventType     = "x-event-type"
	HeaderEventTime     = "x-event-time"
)

func metadataToHeaders(meta *Metadata) map[string]string {
	headers := make(map[string]string)

	if meta == nil {
		return headers
	}

	if !meta.SchemaVersion.IsEmpty() {
		headers[HeaderSchemaVersion] = string(meta.SchemaVersion)
	}

	if len(meta.EventId) > 0 {
		headers[HeaderEventId] = meta.EventId
	}

	if !meta.EventSource.IsEmpty() {
		headers[HeaderEventSource] = string(meta.EventSource)
	}

	if !meta.EventType.IsEmpty() {
		headers[HeaderEventType] = string(meta.EventType)
	}

	if meta.EventTime > 0 {
		headers[HeaderEventTime] = strconv.FormatInt(meta.EventTime, 10)
	}

	return headers
}
