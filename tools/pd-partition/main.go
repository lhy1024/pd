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

package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/codec"
	pdHttp "github.com/tikv/pd/client/http"
	"go.etcd.io/etcd/pkg/transport"
	"go.uber.org/zap"
)

var (
	pdAddr    = flag.String("pd", "127.0.0.1:2379", "pd address")
	dsn       = flag.String("dsn", "root:@tcp(127.0.0.1:4000)/test?parseTime=true", "Database Source Name")
	caPath    = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs")
	certPath  = flag.String("cert", "", "path of file that contains X509 certificate in PEM format")
	keyPath   = flag.String("key", "", "path of file that contains X509 key in PEM format")
	tableName = flag.String("table", "", "Partitioned table to process (required)")
	dbName    = flag.String("db", "test", "Database name")
)

// client is a pd HTTP client
var client pdHttp.Client

type mergePair struct {
	SourceID int64
	TargetID int64
}

type partitionSummary struct {
	name        string
	regionCount int
}

func logPartitionSummary(ctx context.Context, db *sql.DB, dbName, tableName string) {
	query := `
        SELECT
            IFNULL(PARTITION_NAME, 'Global Index / Others') AS partition_name,
            COUNT(DISTINCT REGION_ID) AS region_count
        FROM
            information_schema.TIKV_REGION_STATUS
        WHERE
            DB_NAME = ? AND TABLE_NAME = ?
        GROUP BY
            PARTITION_NAME
        ORDER BY
            PARTITION_NAME
    `
	rows, err := db.QueryContext(ctx, query, dbName, tableName)
	if err != nil {
		log.Error("failed to query partition summary", zap.String("table", tableName), zap.Error(err))
		return
	}
	defer rows.Close()

	var results []partitionSummary
	for rows.Next() {
		var summary partitionSummary
		if err := rows.Scan(&summary.name, &summary.regionCount); err != nil {
			log.Error("failed to scan partition summary row", zap.Error(err))
			continue
		}
		results = append(results, summary)
	}

	if len(results) == 0 {
		log.Info("no physical layout information found for table", zap.String("table", tableName))
		return
	}

	maxWidths := map[string]int{"partition_name": 15, "region_count": 12}
	for _, r := range results {
		if len(r.name) > maxWidths["partition_name"] {
			maxWidths["partition_name"] = len(r.name)
		}
	}
	format := fmt.Sprintf("| %%-%ds | %%-%ds |\n", maxWidths["partition_name"], maxWidths["region_count"])
	separator := fmt.Sprintf("+-%s-+-%s-+\n", strings.Repeat("-", maxWidths["partition_name"]), strings.Repeat("-", maxWidths["region_count"]))

	fmt.Printf("\nPhysical Layout Summary for Table: %s.%s\n", dbName, tableName)
	fmt.Print(separator)
	fmt.Printf(format, "partition_name", "region_count")
	fmt.Print(separator)
	for _, r := range results {
		fmt.Printf(format, r.name, fmt.Sprintf("%d", r.regionCount))
	}
	fmt.Print(separator)
}

func constructTableBoundaryKey(tableID int64) string {
	keyInner := codec.EncodeInt([]byte{'t'}, tableID)
	keyOuter := codec.EncodeBytes([]byte{}, keyInner)
	return strings.ToUpper(hex.EncodeToString(keyOuter))
}

func newPDHttpClient() pdHttp.Client {
	var (
		tlsConfig *tls.Config
		err       error
	)
	if *caPath != "" || *certPath != "" || *keyPath != "" {
		tlsInfo := transport.TLSInfo{
			CertFile:      *certPath,
			KeyFile:       *keyPath,
			TrustedCAFile: *caPath,
		}
		tlsConfig, err = tlsInfo.ClientConfig()
		if err != nil {
			log.Error("failed to create TLS config", zap.Error(err))
			os.Exit(1)
		}
	}
	url := ModifyURLScheme(*pdAddr, tlsConfig)
	return pdHttp.NewClient("pd-partition", []string{url}, pdHttp.WithTLSConfig(tlsConfig))
}

const (
	httpScheme        = "http"
	httpsScheme       = "https"
	httpSchemePrefix  = "http://"
	httpsSchemePrefix = "https://"
)

// ModifyURLScheme modifies the scheme of the URL based on the TLS config.
func ModifyURLScheme(url string, tlsCfg *tls.Config) string {
	if tlsCfg == nil {
		if strings.HasPrefix(url, httpsSchemePrefix) {
			url = httpSchemePrefix + strings.TrimPrefix(url, httpsSchemePrefix)
		} else if !strings.HasPrefix(url, httpSchemePrefix) {
			url = httpSchemePrefix + url
		}
	} else {
		if strings.HasPrefix(url, httpSchemePrefix) {
			url = httpsSchemePrefix + strings.TrimPrefix(url, httpSchemePrefix)
		} else if !strings.HasPrefix(url, httpsSchemePrefix) {
			url = httpsSchemePrefix + url
		}
	}
	return url
}

// TrimHTTPPrefix trims the HTTP/HTTPS prefix from the string.
func TrimHTTPPrefix(str string) string {
	str = strings.TrimPrefix(str, httpSchemePrefix)
	str = strings.TrimPrefix(str, httpsSchemePrefix)
	return str
}

func getRegionsInRange(ctx context.Context, startKeyHex, endKeyHex string) (*pdHttp.RegionsInfo, error) {
	startKey, err := hex.DecodeString(startKeyHex)
	if err != nil {
		return nil, err
	}
	endKey, err := hex.DecodeString(endKeyHex)
	if err != nil {
		return nil, err
	}
	return client.GetRegionsByKeyRange(ctx, pdHttp.NewKeyRange(startKey, endKey), -1)
}

func mergePartitionWorker(ctx context.Context, wg *sync.WaitGroup, db *sql.DB, pdAddr, dbName, tableName, partitionName string, partitionID int64) {
	defer wg.Done()
	logFields := []zap.Field{
		zap.String("table", tableName),
		zap.String("partition", partitionName),
		zap.Int64("partition_id", partitionID),
	}

	// Prepare start and end keys for the partition
	startKeyHex := constructTableBoundaryKey(partitionID)
	endKeyHex := constructTableBoundaryKey(partitionID + 1)

	payload := map[string]any{
		"split_keys": []string{startKeyHex, endKeyHex},
	}
	if err := client.SplitRegions(ctx, payload); err != nil {
		log.Warn("boundary split request failed", append(logFields,
			zap.Strings("split-keys", []string{startKeyHex, endKeyHex}),
			zap.Error(err))...)
	}

	// Merge in rounds
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		resp, err := getRegionsInRange(ctx, startKeyHex, endKeyHex)
		if err != nil {
			log.Error("failed to get current regions, stopping worker", zap.Error(err))
			continue
		}
		if resp.Count <= 1 {
			return
		}

		// check regions
		var pairs []mergePair
		sort.Sort(byStartKeys(*resp))
		for i := 0; i < int(resp.Count)-1; i += 2 {
			if resp.Regions[i].EndKey != resp.Regions[i+1].StartKey {
				log.Error("found a gap between regions, cannot proceed with merging", append(logFields,
					zap.Int64("region1_id", resp.Regions[i].ID),
					zap.String("region1_end_key", resp.Regions[i].EndKey),
					zap.Int64("region2_id", resp.Regions[i+1].ID),
					zap.String("region2_start_key", resp.Regions[i+1].StartKey))...)
				continue
			}
			pairs = append(pairs, mergePair{
				SourceID: resp.Regions[i+1].ID,
				TargetID: resp.Regions[i].ID,
			})
		}

		executeMergeRound(ctx, pairs, logFields)
	}
}

func executeMergeRound(ctx context.Context, pairs []mergePair, logFields []zap.Field) {
	if len(pairs) == 0 {
		return
	}
	for _, pair := range pairs {
		input := map[string]any{
			"name":             "merge-region",
			"source_region_id": pair.SourceID,
			"target_region_id": pair.TargetID,
		}
		if err := client.CreateOperators(ctx, input); err != nil {
			log.Error("failed to submit merge operator, skipping pair", append(logFields,
				zap.Any("pair", pair), zap.Error(err))...)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		successCount := 0
		for _, pair := range pairs {
			resp, err := client.GetOperatorsByRegion(ctx, uint64(pair.TargetID))
			if err != nil {
				continue
			}
			if strings.Contains(resp, "SUCCESS") {
				successCount++
			}
		}
		if successCount == len(pairs) {
			return
		}
		select {
		case <-ticker.C:

		case <-ctx.Done():
			return
		}
	}
}

type byStartKeys pdHttp.RegionsInfo

func (a byStartKeys) Len() int {
	return int(a.Count)
}

func (a byStartKeys) Less(i, j int) bool {
	return a.Regions[i].StartKey < a.Regions[j].StartKey
}

func (a byStartKeys) Swap(i, j int) {
	a.Regions[i], a.Regions[j] = a.Regions[j], a.Regions[i]
}

func getPartitions(ctx context.Context, db *sql.DB, dbName, tableName string) (map[int64]string, error) {
	query := `
        SELECT
            p.PARTITION_NAME,
            p.TIDB_PARTITION_ID
        FROM
            information_schema.PARTITIONS p
        JOIN (
            SELECT PARTITION_NAME
            FROM information_schema.TIKV_REGION_STATUS
            WHERE DB_NAME = ? AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL
            GROUP BY PARTITION_NAME
            HAVING COUNT(DISTINCT REGION_ID) > 1
        ) AS regions_to_merge ON p.PARTITION_NAME = regions_to_merge.PARTITION_NAME
        WHERE
            p.TABLE_SCHEMA = ? AND p.TABLE_NAME = ?
    `
	rows, err := db.QueryContext(ctx, query, dbName, tableName, dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query initial partitions: %w", err)
	}
	defer rows.Close()

	partitions := make(map[int64]string)
	for rows.Next() {
		var pName string
		var pID int64
		if err := rows.Scan(&pName, &pID); err != nil {
			return nil, err
		}
		partitions[pID] = pName
	}
	return partitions, nil
}

func main() {
	// parse command line arguments
	flag.Parse()
	if *tableName == "" {
		log.Fatal("required flag not provided: -table")
	}

	// initialize clients
	client = newPDHttpClient()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Hour)
	defer cancel()

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		log.Fatal("failed to open database connection", zap.Error(err))
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		log.Fatal("failed to connect to database", zap.Error(err))
	}
	logPartitionSummary(ctx, db, *dbName, *tableName)
	start := time.Now()
	// check partitions
	partitions, err := getPartitions(ctx, db, *dbName, *tableName)
	if err != nil {
		log.Fatal("error getting partitions to process", zap.Error(err))
	}
	if len(partitions) == 0 {
		log.Info("no partitions to process")
		return
	}
	var wg sync.WaitGroup
	for id, name := range partitions {
		wg.Add(1)
		go mergePartitionWorker(ctx, &wg, db, *pdAddr, *dbName, *tableName, name, id)
	}
	wg.Wait()
	log.Info("partition merge complete", zap.Int("partitions", len(partitions)), zap.Duration("elapsed", time.Since(start)))
	logPartitionSummary(ctx, db, *dbName, *tableName)
}
