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
	"time" // Event is thing that happened during a Span's lifetime.

	"go.opentelemetry.io/otel/label"
)

type Event struct {
	// Name is the name of this event
	Name string

	// Attributes describe the aspects of the event.
	Attributes []label.KeyValue

	// Time is the time at which this event was recorded.
	Time time.Time
}
