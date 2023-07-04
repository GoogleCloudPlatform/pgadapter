/*
Copyright 2023 Google LLC
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pgadapter

import (
	"context"
	"testing"
)

func TestStart(t *testing.T) {
	pgadapter, err := Start(context.Background(), Config{})
	if err != nil {
		t.Fatalf("failed to start PGAdapter: %v", err)
	}
	port, err := pgadapter.GetHostPort()
	if err != nil {
		t.Fatalf("failed to get port from PGAdapter: %v", err)
	}
	if port == 0 {
		t.Fatal("Port should be non-zero", err)
	}
	err = pgadapter.Stop()
	if err != nil {
		t.Fatalf("failed to stop PGAdapter: %v", err)
	}
}
