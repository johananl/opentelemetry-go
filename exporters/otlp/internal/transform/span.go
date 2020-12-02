// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transform

import (
	"go.opentelemetry.io/otel/codes"
	tracepb "go.opentelemetry.io/otel/exporters/otlp/internal/opentelemetry-proto-gen/trace/v1"

	"go.opentelemetry.io/otel/label"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	maxMessageEventsPerSpan = 128
)

// SpanData transforms a slice of ReadOnlySpans into a slice of OTLP
// ResourceSpans.
func SpanData(spans []sdktrace.ReadOnlySpan) []*tracepb.ResourceSpans {
	if len(spans) == 0 {
		return nil
	}

	rsm := make(map[label.Distinct]*tracepb.ResourceSpans)

	type ilsKey struct {
		r  label.Distinct
		il instrumentation.Library
	}
	ilsm := make(map[ilsKey]*tracepb.InstrumentationLibrarySpans)

	var resources int
	for _, s := range spans {
		if s == nil {
			continue
		}

		rKey := s.Resource().Equivalent()
		iKey := ilsKey{
			r:  rKey,
			il: s.InstrumentationLibrary(),
		}
		ils, iOk := ilsm[iKey]
		if !iOk {
			// Either the resource or instrumentation library were unknown.
			ils = &tracepb.InstrumentationLibrarySpans{
				InstrumentationLibrary: instrumentationLibrary(s.InstrumentationLibrary()),
				Spans:                  []*tracepb.Span{},
			}
		}
		ils.Spans = append(ils.Spans, span(s))
		ilsm[iKey] = ils

		rs, rOk := rsm[rKey]
		if !rOk {
			resources++
			// The resource was unknown.
			rs = &tracepb.ResourceSpans{
				Resource:                    Resource(s.Resource()),
				InstrumentationLibrarySpans: []*tracepb.InstrumentationLibrarySpans{ils},
			}
			rsm[rKey] = rs
			continue
		}

		// The resource has been seen before. Check if the instrumentation
		// library lookup was unknown because if so we need to add it to the
		// ResourceSpans. Otherwise, the instrumentation library has already
		// been seen and the append we did above will be included it in the
		// InstrumentationLibrarySpans reference.
		if !iOk {
			rs.InstrumentationLibrarySpans = append(rs.InstrumentationLibrarySpans, ils)
		}
	}

	// Transform the categorized map into a slice
	rss := make([]*tracepb.ResourceSpans, 0, resources)
	for _, rs := range rsm {
		rss = append(rss, rs)
	}
	return rss
}

// span transforms a ReadOnlySpan into an OTLP span.
func span(s sdktrace.ReadOnlySpan) *tracepb.Span {
	if s == nil {
		return nil
	}

	sc := s.SpanContext()

	ts := &tracepb.Span{
		TraceId:           sc.TraceID[:],
		SpanId:            sc.SpanID[:],
		Status:            status(s.StatusCode(), s.StatusMessage()),
		StartTimeUnixNano: uint64(s.StartTime().UnixNano()),
		EndTimeUnixNano:   uint64(s.EndTime().UnixNano()),
		Links:             links(s.Links()),
		Kind:              spanKind(s.SpanKind()),
		Name:              s.Name(),
		Attributes:        Attributes(s.Attributes()),
		Events:            spanEvents(s.Events()),
		// TODO (rghetia): Add Tracestate: when supported.
		DroppedAttributesCount: uint32(s.DroppedAttributes()),
		DroppedEventsCount:     uint32(s.DroppedEvents()),
		DroppedLinksCount:      uint32(s.DroppedLinks()),
	}

	if s.Parent().SpanID.IsValid() {
		psid := s.Parent().SpanID
		ts.ParentSpanId = psid[:]
	}

	return ts
}

// status transform a span code and message into an OTLP span status.
func status(status codes.Code, message string) *tracepb.Status {
	var c tracepb.Status_StatusCode
	switch status {
	case codes.Error:
		c = tracepb.Status_STATUS_CODE_ERROR
	default:
		c = tracepb.Status_STATUS_CODE_OK
	}
	return &tracepb.Status{
		Code:    c,
		Message: message,
	}
}

// links transforms span Links to OTLP span links.
func links(links []trace.Link) []*tracepb.Span_Link {
	if len(links) == 0 {
		return nil
	}

	sl := make([]*tracepb.Span_Link, 0, len(links))
	for _, otLink := range links {
		// This redefinition is necessary to prevent otLink.*ID[:] copies
		// being reused -- in short we need a new otLink per iteration.
		otLink := otLink

		sl = append(sl, &tracepb.Span_Link{
			TraceId:    otLink.TraceID[:],
			SpanId:     otLink.SpanID[:],
			Attributes: Attributes(otLink.Attributes),
		})
	}
	return sl
}

// spanEvents transforms span Events to an OTLP span events.
func spanEvents(es []sdktrace.Event) []*tracepb.Span_Event {
	if len(es) == 0 {
		return nil
	}

	evCount := len(es)
	if evCount > maxMessageEventsPerSpan {
		evCount = maxMessageEventsPerSpan
	}
	events := make([]*tracepb.Span_Event, 0, evCount)
	messageEvents := 0

	// Transform message events
	for _, e := range es {
		if messageEvents >= maxMessageEventsPerSpan {
			break
		}
		messageEvents++
		events = append(events,
			&tracepb.Span_Event{
				Name:         e.Name,
				TimeUnixNano: uint64(e.Time.UnixNano()),
				Attributes:   Attributes(e.Attributes),
				// TODO (rghetia) : Add Drop Counts when supported.
			},
		)
	}

	return events
}

// spanKind transforms a SpanKind to an OTLP span kind.
func spanKind(kind trace.SpanKind) tracepb.Span_SpanKind {
	switch kind {
	case trace.SpanKindInternal:
		return tracepb.Span_SPAN_KIND_INTERNAL
	case trace.SpanKindClient:
		return tracepb.Span_SPAN_KIND_CLIENT
	case trace.SpanKindServer:
		return tracepb.Span_SPAN_KIND_SERVER
	case trace.SpanKindProducer:
		return tracepb.Span_SPAN_KIND_PRODUCER
	case trace.SpanKindConsumer:
		return tracepb.Span_SPAN_KIND_CONSUMER
	default:
		return tracepb.Span_SPAN_KIND_UNSPECIFIED
	}
}
