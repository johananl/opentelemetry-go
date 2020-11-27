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

package trace // import "go.opentelemetry.io/otel/sdk/trace"

import (
	"math/rand"
	"sync"

	"go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/otel/sdk/trace/internal"
)

// TODO: Is it OK to export this type?
type DefaultIDGenerator struct {
	sync.Mutex
	RandSource *rand.Rand
}

var _ internal.IDGenerator = &DefaultIDGenerator{}

// NewSpanID returns a non-zero span ID from a randomly-chosen sequence.
func (gen *DefaultIDGenerator) NewSpanID() trace.SpanID {
	gen.Lock()
	defer gen.Unlock()
	sid := trace.SpanID{}
	gen.RandSource.Read(sid[:])
	return sid
}

// NewTraceID returns a non-zero trace ID from a randomly-chosen sequence.
// mu should be held while this function is called.
func (gen *DefaultIDGenerator) NewTraceID() trace.TraceID {
	gen.Lock()
	defer gen.Unlock()
	tid := trace.TraceID{}
	gen.RandSource.Read(tid[:])
	return tid
}
