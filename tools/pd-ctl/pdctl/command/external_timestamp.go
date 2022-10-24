// Copyright 2022 TiKV Project Authors.
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

package command

import (
	"net/http"

	"github.com/spf13/cobra"
)

var (
	externalTimestampPrefix = "pd/api/v1/external-timestamp"
)

// NewExternalTimestampCommand return external timestamp subcommand of rootCmd
func NewExternalTimestampCommand() *cobra.Command {
	l := &cobra.Command{
		Use:   "external-timestamp",
		Short: "show external timestamp",
		Run:   ShowExternalTimestamp,
	}
	return l
}

// ShowExternalTimestamp show external timestamp
func ShowExternalTimestamp(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, externalTimestampPrefix, http.MethodGet, http.Header{})
	if err != nil {
		cmd.Printf("Failed to get external timestamp: %s\n", err)
		return
	}
	cmd.Println(r)
}