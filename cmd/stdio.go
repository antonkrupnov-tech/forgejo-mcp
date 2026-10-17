// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright © 2025 Ronmi Ren <ronmi.ren@gmail.com>

package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/raohwork/forgejo-mcp/internal/httpx"
	"github.com/raohwork/forgejo-mcp/tools"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// stdioCmd represents the stdio command
var stdioCmd = &cobra.Command{
	Use:   "stdio",
	Short: "Run MCP server in stdio mode",
	Long: `Run the Forgejo MCP server using stdio transport for communication
with MCP clients through JSON-RPC over standard input/output.

This transport mode is ideal for:
  - Local integrations and development
  - Direct process communication
  - Applications that can spawn child processes

Example:
  forgejo-mcp stdio --server https://git.example.com --token your_token`,
	Run: func(cmd *cobra.Command, args []string) {
		base := viper.GetString("server")
		token := viper.GetString("token")

		if base == "" || token == "" {
			cmd.Help()
			os.Exit(1)
		}

		resilient := httpx.NewResilientClient()
		cl, err := tools.NewClient(base, token, "", resilient)
		if err != nil {
			// The startup version check is advisory. It performs one API
			// call, so a rate-limited or briefly unreachable Forgejo must
			// not abort the whole MCP server. If the SDK still handed us a
			// usable client (unknown-version errors do), start with it;
			// otherwise rebuild skipping the check, the same established
			// behaviour as http multiuser mode.
			fmt.Fprintf(os.Stderr, "warning: Forgejo version check failed, continuing without it: %v\n", err)
			if cl == nil {
				cl, err = tools.NewClient(base, token, "9", resilient)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error creating SDK client: %v\n", err)
					os.Exit(1)
				}
			}
		}

		server := createServer(cl)
		err = server.Run(context.TODO(), mcp.NewStdioTransport())
		fmt.Fprintf(os.Stderr, "Server exited with error: %v\n", err)
		if err != nil {
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(stdioCmd)
}
