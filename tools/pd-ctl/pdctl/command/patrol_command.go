// Copyright 2025 TiKV Project Authors.
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
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/spf13/cobra"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/router"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const msgSize = 16 * units.MiB
const defaultLimit = 8192

// PatrolResult defines the structure for JSON output of each processed region.
type PatrolResult struct {
	RegionID    uint64 `json:"region_id"`
	Key         string `json:"key"`
	TableID     int64  `json:"table_id"`
	Status      string `json:"status"`
	Description string `json:"description,omitempty"`
}

type PatrolResults struct {
	ScanCount    int            `json:"scan_count"`
	ScanDuration time.Duration  `json:"scan_duration"`
	Count        int            `json:"count"`
	Results      []PatrolResult `json:"results"`
}

// NewPatrolCommand creates the patrol subcommand.
func NewPatrolCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "patrol",
		Short: "Patrol regions to find invalid keys and optionally merge them.",
		Run:   patrolCommandFunc,
	}
	cmd.Flags().Int("limit", defaultLimit, "Limit of regions to scan per batch. If <= 0, it means no limit")
	cmd.Flags().Bool("enable-auto-merge", false, "Enable automatic region merge for invalid regions.")
	return cmd
}

func patrolCommandFunc(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	limit, _ := cmd.Flags().GetInt("limit")
	enableAutoMerge, _ := cmd.Flags().GetBool("enable-auto-merge")

	opts, err := getSecurityOpt(cmd)
	if err != nil {
		cmd.Printf("Failed to parse TLS options: %v\n", err)
		return
	}
	addr := getEndpoints(cmd)
	pdCli, err := pd.NewClientWithContext(ctx, caller.TestComponent, addr, opts,
		opt.WithGRPCDialOptions(grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(msgSize))))
	if err != nil {
		cmd.Printf("Failed to create pd client: %v\n", err)
		return
	}
	defer pdCli.Close()

	var patrolResults *PatrolResults
	results := make([]PatrolResult, 0)
	invalidRegions := make(map[uint64]*router.Region)

	startKey := []byte{}
	count := 0
	startTime := time.Now()
	for {
		select {
		case <-ctx.Done():
			cmd.Printf("Patrol command cancelled: %v\n", ctx.Err())
			return
		default:
		}
		regions, err := pdCli.ScanRegions(ctx, startKey, nil, limit)
		if err != nil {
			cmd.Printf("Failed to scan regions: %v\n", err)
			return
		}
		for _, region := range regions {
			tableID, found, _ := checkRegion(region)
			if found {
				invalidRegions[region.Meta.GetId()] = region
				if !enableAutoMerge {
					results = append(results, PatrolResult{
						RegionID: region.Meta.GetId(),
						Key:      hex.EncodeToString(region.Meta.GetEndKey()),
						TableID:  tableID,
						Status:   "Merge Skipped",
					})
				}
			}
		}
		count += len(regions)
		if len(regions) == 0 {
			break
		}
		endKey := regions[len(regions)-1].Meta.GetEndKey()
		if len(endKey) == 0 || bytes.Compare(startKey, endKey) >= 0 {
			break
		}
		startKey = endKey
	}

	if !enableAutoMerge {
		patrolResults = &PatrolResults{
			ScanCount:    count,
			ScanDuration: time.Since(startTime),
			Count:        len(results),
			Results:      results,
		}
	} else {
		for regionID, region := range invalidRegions {
			endKeyHex := hex.EncodeToString(region.Meta.GetEndKey())
			// Find sibling
			siblingRegions, err := PDCli.GetRegionsSiblingByID(ctx, regionID)
			if err != nil {
				results = append(results, PatrolResult{
					RegionID:    regionID,
					Key:         endKeyHex,
					Status:      "Merge Failed",
					Description: fmt.Sprintf("Failed to find sibling region: %v", err),
				})
				continue
			}
			if siblingRegions.Count == 0 {
				results = append(results, PatrolResult{
					RegionID:    regionID,
					Key:         endKeyHex,
					Status:      "Merge Failed",
					Description: "No sibling region found.",
				})
				continue
			}
			// Create merge operator
			rightRegion := siblingRegions.Regions[siblingRegions.Count-1]
			rightRegionID := uint64(rightRegion.ID)
			err = PDCli.CreateMergeOperator(ctx, regionID, rightRegionID)
			if err != nil {
				results = append(results, PatrolResult{
					RegionID:    regionID,
					Key:         endKeyHex,
					Status:      "Merge Failed",
					Description: fmt.Sprintf("Failed to send merge operator: %v", err),
				})
			} else {
				results = append(results, PatrolResult{
					RegionID:    regionID,
					Key:         endKeyHex,
					Status:      "Merge Requested",
					Description: fmt.Sprintf("Merge with region %d requested.", rightRegionID),
				})
			}
		}
	}
	// Print final JSON output
	finalOutput, err := json.MarshalIndent(patrolResults, "", "  ")
	if err != nil {
		cmd.Printf("Failed to marshal patrol results to JSON: %v\n", err)
		return
	}
	cmd.Println(string(finalOutput))
}

func checkRegion(region *router.Region) (tableID int64, found bool, err error) {
	regionID := region.Meta.GetId()
	key := region.Meta.GetEndKey()
	hexKeyStr := hex.EncodeToString(key)
	if len(key) == 0 {
		return
	}
	// Add a panic recovery to ensure we don't crash the entire program
	defer func() {
		if r := recover(); r != nil {
			log.Error("recovered from panic in checkRegion",
				zap.Uint64("region_id", regionID),
				zap.String("end_key_hex", hexKeyStr),
				zap.Any("error", r))
		}
	}()
	rootNode := N("key", key)
	rootNode.Expand()
	isInvalid := hasInvalidPatternRecursive(rootNode)
	if isInvalid {
		found = true
		tableID, _, err = extractTableIDRecursive(rootNode)
		if err != nil {
			return
		}
		return
	}
	return
}

// hasInvalidPatternRecursive recursively searches the Node tree for the specific key pattern.
func hasInvalidPatternRecursive(node *Node) bool {
	for _, variant := range node.variants {
		// Target pattern path:
		// Node (rootNode or child of DecodeHex)
		//  -> Variant (method: "decode hex key") [It has been finished in Expand()]
		//    -> Node (val: hex_decoded_bytes)
		//       -> Variant (method: "decode mvcc key")
		//          -> Node (val: mvcc_key_body, call as mvccBodyNode)
		//             -> Variant (method: "table row key", call as tableRowVariant)
		//                -> Node (typ: "table_id", ...)
		//                -> Node (typ: "index_values" or "row_id", val: row_data, call as rowDataNode)
		//                   -> Variant (method: "decode index values")

		if variant.method == "decode mvcc key" {
			for _, mvccBodyNode := range variant.children {
				for _, tableRowVariant := range mvccBodyNode.variants {
					if tableRowVariant.method == "table row key" {
						// According to DecodeTableRow, it should have 2 children:
						// children[0] is N("table_id", ...)
						// children[1] is N(handleTyp, row_data_bytes) -> this is rowDataNode (Node_B)
						if len(tableRowVariant.children) == 2 {
							rowDataNode := tableRowVariant.children[1]
							// Confirm if rowDataNode's type is as expected, which is determined by DecodeTableRow's handleTyp.
							if rowDataNode.typ == "index_values" || rowDataNode.typ == "row_id" {
								// Condition 1: Does row data end with non \x00?
								// And we only care about the 9 bytes of the row data.
								if !(len(rowDataNode.val) == 9 && rowDataNode.val[len(rowDataNode.val)-1] != '\x00') {
									continue
								}
								// Condition 2: Does rowDataNode have extra output?
								for _, rdnVariant := range rowDataNode.variants {
									if rdnVariant.method == "decode index values" {
										return true
									}
								}
							}
						}
					}
				}
			}
		}

		// We need to recursively check all children of the current variant.
		if slices.ContainsFunc(variant.children, hasInvalidPatternRecursive) {
			return true
		}
	}
	return false
}

// extractTableIDRecursive recursively searches the expanded Node tree to try and extract and decode the table ID.
func extractTableIDRecursive(node *Node) (tableID int64, found bool, err error) {
	for _, variant := range node.variants {
		if variant.method == "decode mvcc key" {
			for _, mvccChildNode := range variant.children {
				for _, detailVariant := range mvccChildNode.variants {
					if detailVariant.method == "table prefix" || detailVariant.method == "table row key" {
						// Both of these variant types should have a child Node with typ "table_id".
						// its `.val` contains bytes decodable by `codec.DecodeInt()`.
						for _, childOfDetail := range detailVariant.children {
							if childOfDetail.typ == "table_id" {
								_, id, decodeErr := codec.DecodeInt(childOfDetail.val)
								if decodeErr == nil {
									return id, true, nil
								}
								return 0, false, fmt.Errorf("failed to decode table_id node (type: %s, value_hex: %x): %w",
									childOfDetail.typ, childOfDetail.val, decodeErr)
							}
						}
					}
				}
			}
		}

		for _, childNode := range variant.children {
			id, found, err := extractTableIDRecursive(childNode)
			if err != nil {
				return 0, false, err
			}
			if found {
				return id, true, nil
			}
		}
	}
	return 0, false, nil
}

func getSecurityOpt(cmd *cobra.Command) (opt pd.SecurityOption, err error) {
	caPath, err := cmd.Flags().GetString("cacert")
	if err != nil || len(caPath) == 0 {
		return opt, err
	}
	certPath, err := cmd.Flags().GetString("cert")
	if err != nil {
		return opt, err
	}
	keyPath, err := cmd.Flags().GetString("key")
	if err != nil {
		return opt, err
	}
	opt.CAPath = caPath
	opt.CertPath = certPath
	opt.KeyPath = keyPath
	return opt, nil
}
