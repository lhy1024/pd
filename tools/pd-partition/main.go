package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/codec"
	pdHttp "github.com/tikv/pd/client/http"
	"go.etcd.io/etcd/pkg/transport"
	"go.uber.org/zap"
)

var (
	pdAddr       = flag.String("pd", "127.0.0.1:2379", "pd address")
	dsn          = flag.String("dsn", "root:@tcp(127.0.0.1:4000)/test?parseTime=true", "Database Source Name")
	caPath       = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs")
	certPath     = flag.String("cert", "", "path of file that contains X509 certificate in PEM format")
	keyPath      = flag.String("key", "", "path of file that contains X509 key in PEM format")
	tableName    = flag.String("table", "", "Table to process (partitioned or non-partitioned)")
	dbName       = flag.String("db", "test", "Database name")
	allTables    = flag.Bool("all-tables", false, "Process all tables in the specified database")
	tableTimeout = flag.Duration("timeout-per-table", 10*time.Minute, "Timeout for processing a single table (e.g., 15m, 1h)")
)

// client is a pd HTTP client
var client pdHttp.Client

type mergePair struct {
	SourceID int64
	TargetID int64
}

// partitionInfo holds the raw data from the database for a partition.
type partitionInfo struct {
	name        string
	regionCount int
	avgSizeMB   float64
}

// partitionResult holds the combined before/after state for a single partition.
type partitionResult struct {
	partitionName  string
	beforeCount    int
	afterCount     int
	avgSizeMBAfter float64
}

// tableResult holds the result for an entire table's processing.
type tableResult struct {
	tableName  string
	partitions []partitionResult
	elapsed    time.Duration
	skipped    bool
	err        error
}

// parseSizeToMB converts a size string (e.g., "144MiB", "10GiB") to megabytes.
func parseSizeToMB(sizeStr string) (int64, error) {
	sizeStr = strings.ToUpper(strings.TrimSpace(sizeStr))
	var multiplier float64
	var unit string

	if strings.HasSuffix(sizeStr, "GIB") {
		multiplier = 1024
		unit = "GIB"
	} else if strings.HasSuffix(sizeStr, "MIB") {
		multiplier = 1
		unit = "MIB"
	} else if strings.HasSuffix(sizeStr, "KIB") {
		multiplier = 1.0 / 1024.0
		unit = "KIB"
	} else if strings.HasSuffix(sizeStr, "B") {
		multiplier = 1.0 / (1024.0 * 1024.0)
		unit = "B"
	} else {
		multiplier = 1
	}

	numStr := strings.TrimSuffix(sizeStr, unit)
	size, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size number: %s", numStr)
	}

	return int64(size * multiplier), nil
}

// getSplitSizeMB fetches the region-split-size from TiKV config and returns it in MB.
func getSplitSizeMB(ctx context.Context, db *sql.DB) (int64, error) {
	var configValue string
	query := `SHOW CONFIG WHERE Type='tikv' AND Name LIKE '%region-split-size'`
	err := db.QueryRowContext(ctx, query).Scan(new(string), new(string), new(string), &configValue)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("could not find TiKV config for 'region-split-size'. Please check TiDB version and privileges")
		}
		return 0, err
	}

	splitSizeMB, err := parseSizeToMB(configValue)
	if err != nil {
		return 0, fmt.Errorf("failed to parse region-split-size '%s': %w", configValue, err)
	}
	return splitSizeMB, nil
}

func tableExists(ctx context.Context, db *sql.DB, dbName, tableName string) (bool, error) {
	query := `SELECT COUNT(1) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`
	var count int
	err := db.QueryRowContext(ctx, query, dbName, tableName).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func getPartitionSummary(ctx context.Context, db *sql.DB, dbName, tableName string) ([]partitionInfo, error) {
	query := `
        SELECT
            IFNULL(PARTITION_NAME, 'NON_PARTITIONED') AS partition_name,
            COUNT(DISTINCT REGION_ID) AS region_count,
            IFNULL(AVG(APPROXIMATE_SIZE), 0) AS avg_size_mb
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
		log.Error("failed to query partition summary", zap.String("db", dbName), zap.String("table", tableName), zap.Error(err))
		return nil, err
	}
	defer rows.Close()

	var results []partitionInfo
	for rows.Next() {
		var summary partitionInfo
		if err := rows.Scan(&summary.name, &summary.regionCount, &summary.avgSizeMB); err != nil {
			log.Error("failed to scan partition summary row", zap.Error(err))
			continue
		}
		results = append(results, summary)
	}
	return results, nil
}

func printUnifiedSummary(results []tableResult) {
	if len(results) == 0 {
		log.Info("No tables were processed.")
		return
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].tableName < results[j].tableName
	})

	maxWidths := map[string]int{
		"table": 5, "part": 9, "before": 14, "after": 13, "avg_size_after": 18, "elapsed": 12, "status": 12,
	}

	for _, r := range results {
		if len(r.tableName) > maxWidths["table"] {
			maxWidths["table"] = len(r.tableName)
		}
		for _, p := range r.partitions {
			if len(p.partitionName) > maxWidths["part"] {
				maxWidths["part"] = len(p.partitionName)
			}
		}
	}

	headerFmt := fmt.Sprintf("| %%-%ds | %%-%ds | %%-%ds | %%-%ds | %%-%ds | %%-%ds | %%-%ds |\n",
		maxWidths["table"], maxWidths["part"], maxWidths["before"], maxWidths["after"], maxWidths["avg_size_after"], maxWidths["elapsed"], maxWidths["status"])
	rowFmt := fmt.Sprintf("| %%-%ds | %%-%ds | %%-%dd | %%-%dd | %%-%ds | %%-%ds | %%-%ds |\n",
		maxWidths["table"], maxWidths["part"], maxWidths["before"], maxWidths["after"], maxWidths["avg_size_after"], maxWidths["elapsed"], maxWidths["status"])
	separator := fmt.Sprintf("+-%s-+-%s-+-%s-+-%s-+-%s-+-%s-+-%s-+\n",
		strings.Repeat("-", maxWidths["table"]), strings.Repeat("-", maxWidths["part"]), strings.Repeat("-", maxWidths["before"]),
		strings.Repeat("-", maxWidths["after"]), strings.Repeat("-", maxWidths["avg_size_after"]), strings.Repeat("-", maxWidths["elapsed"]), strings.Repeat("-", maxWidths["status"]))

	fmt.Print("\n--- Overall Merge Summary ---\n")
	fmt.Print(separator)
	fmt.Printf(headerFmt, "Table", "Partition", "Regions Before", "Regions After", "Avg Size After(MB)", "Elapsed (s)", "Status")
	fmt.Print(separator)

	var errors []tableResult
	for _, r := range results {
		var status string

		if r.err == context.DeadlineExceeded {
			status = "Timeout"
		} else if r.err == context.Canceled {
			status = "Canceled"
		} else if r.err != nil {
			errors = append(errors, r)
			continue
		} else if r.skipped {
			status = "Skipped"
		} else {
			var regionsChanged bool
			for _, p := range r.partitions {
				if p.beforeCount != p.afterCount {
					regionsChanged = true
					break
				}
			}
			if regionsChanged {
				status = "Success"
			} else {
				status = "No Action"
			}
		}

		elapsedStr := fmt.Sprintf("%.2f", r.elapsed.Seconds())
		for i, p := range r.partitions {
			tableName := r.tableName
			if i > 0 {
				tableName = ""
			}
			avgSizeStr := fmt.Sprintf("%.1f", p.avgSizeMBAfter)
			fmt.Printf(rowFmt, tableName, p.partitionName, p.beforeCount, p.afterCount, avgSizeStr, elapsedStr, status)
		}
	}
	fmt.Print(separator)

	if len(errors) > 0 {
		fmt.Print("\n--- Other Errors ---\n")
		for _, r := range errors {
			log.Error("failed to process table", zap.String("table", r.tableName), zap.Error(r.err))
		}
	}
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
			CertFile: *certPath, KeyFile: *keyPath, TrustedCAFile: *caPath,
		}
		tlsConfig, err = tlsInfo.ClientConfig()
		if err != nil {
			log.Fatal("failed to create TLS config", zap.Error(err))
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

func mergePartitionWorker(ctx context.Context, wg *sync.WaitGroup, db *sql.DB, dbName, tableName, partitionName string, partitionID int64, splitSizeMB int64) {
	defer wg.Done()
	logFields := []zap.Field{
		zap.String("db", dbName), zap.String("table", tableName), zap.String("partition", partitionName), zap.Int64("partition_id", partitionID),
	}

	startKeyHex := constructTableBoundaryKey(partitionID)
	endKeyHex := constructTableBoundaryKey(partitionID + 1)

	payload := map[string]any{"split_keys": []string{startKeyHex, endKeyHex}}
	if err := client.SplitRegions(ctx, payload); err != nil {
		log.Warn("boundary split request failed", append(logFields, zap.Strings("split-keys", []string{startKeyHex, endKeyHex}), zap.Error(err))...)
	}

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

		var pairs []mergePair
		sort.Sort(byStartKeys(*resp))
		for i := 1; i < int(resp.Count); i += 2 {
			sourceRegion := resp.Regions[i-1]
			targetRegion := resp.Regions[i]

			if sourceRegion.EndKey != targetRegion.StartKey {
				log.Error("found a gap between regions, cannot proceed with merging", append(logFields,
					zap.Int64("region1_id", sourceRegion.ID), zap.String("region1_end_key", sourceRegion.EndKey),
					zap.Int64("region2_id", targetRegion.ID), zap.String("region2_start_key", targetRegion.StartKey))...)
				continue
			}

			combinedSize := sourceRegion.ApproximateSize + targetRegion.ApproximateSize
			if splitSizeMB > 0 && combinedSize >= splitSizeMB {
				continue
			}
			pairs = append(pairs, mergePair{SourceID: sourceRegion.ID, TargetID: targetRegion.ID})
		}
		if len(pairs) == 0 {
			return
		}
		executeMergeRound(ctx, pairs, logFields)
	}
}

func executeMergeRound(ctx context.Context, pairs []mergePair, logFields []zap.Field) {
	if len(pairs) == 0 {
		return
	}
	operatorRegions := make([]int64, 0, len(pairs))
	for _, pair := range pairs {
		input := map[string]any{
			"name":             "merge-region",
			"source_region_id": pair.SourceID,
			"target_region_id": pair.TargetID,
		}
		if err := client.CreateOperators(ctx, input); err != nil {
			if !strings.Contains(err.Error(), "ErrRegionAbnormalPeer") {
				log.Error("failed to submit merge operator, skipping pair", append(logFields,
					zap.Any("pair", pair), zap.Error(err))...)
			}
		} else {
			operatorRegions = append(operatorRegions, pair.TargetID)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		successCount := 0
		for _, region := range operatorRegions {
			resp, err := client.GetOperatorsByRegion(ctx, uint64(region))
			if err != nil {
				continue
			}
			if strings.Contains(resp, "SUCCESS") {
				successCount++
			}
		}
		if successCount >= len(operatorRegions) {
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

func (a byStartKeys) Len() int           { return int(a.Count) }
func (a byStartKeys) Less(i, j int) bool { return a.Regions[i].StartKey < a.Regions[j].StartKey }
func (a byStartKeys) Swap(i, j int)      { a.Regions[i], a.Regions[j] = a.Regions[j], a.Regions[i] }

func getPartitions(ctx context.Context, db *sql.DB, dbName, tableName string) (map[int64]string, error) {
	query := `
        SELECT p.PARTITION_NAME, p.TIDB_PARTITION_ID
        FROM information_schema.PARTITIONS p
        JOIN (
            SELECT PARTITION_NAME
            FROM information_schema.TIKV_REGION_STATUS
            WHERE DB_NAME = ? AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL
            GROUP BY PARTITION_NAME
            HAVING COUNT(DISTINCT REGION_ID) > 1
        ) AS regions_to_merge ON p.PARTITION_NAME = regions_to_merge.PARTITION_NAME
        WHERE p.TABLE_SCHEMA = ? AND p.TABLE_NAME = ?
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

func isPartitionedTable(ctx context.Context, db *sql.DB, dbName, tableName string) (bool, error) {
	query := `SELECT COUNT(1) FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL`
	var count int
	err := db.QueryRowContext(ctx, query, dbName, tableName).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func getTableInfo(ctx context.Context, db *sql.DB, dbName, tableName string) (tableID int64, err error) {
	query := `SELECT TIDB_TABLE_ID FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`
	err = db.QueryRowContext(ctx, query, dbName, tableName).Scan(&tableID)
	return
}

func getAllTables(ctx context.Context, db *sql.DB, dbName string) ([]string, error) {
	query := `SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?`
	rows, err := db.QueryContext(ctx, query, dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to query all tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}
	return tables, nil
}

func processTable(ctx context.Context, db *sql.DB, dbName, tableName string, splitSizeMB int64) tableResult {
	tableCtx, cancel := context.WithTimeout(ctx, *tableTimeout)
	defer cancel()

	start := time.Now()
	res := tableResult{tableName: tableName}

	if ctx.Err() != nil {
		res.err = ctx.Err()
		res.skipped = true
		return res
	}

	exists, err := tableExists(tableCtx, db, dbName, tableName)
	if err != nil {
		res.err = fmt.Errorf("failed to check if table exists: %w", err)
		return res
	}
	if !exists {
		res.err = fmt.Errorf("table does not exist")
		return res
	}

	beforeSummary, err := getPartitionSummary(tableCtx, db, dbName, tableName)
	if err != nil {
		res.err = fmt.Errorf("failed to get 'before' summary: %w", err)
		return res
	}
	if len(beforeSummary) == 0 {
		res.skipped = true
		res.elapsed = time.Since(start)
		res.partitions = []partitionResult{{partitionName: "N/A", beforeCount: 0, afterCount: 0}}
		return res
	}

	isPartitioned, err := isPartitionedTable(tableCtx, db, dbName, tableName)
	if err != nil {
		res.err = fmt.Errorf("failed to check if table is partitioned: %w", err)
		return res
	}
	var wg sync.WaitGroup
	if isPartitioned {
		log.Info("table is partitioned", zap.String("db", dbName), zap.String("table", tableName))
		partitionsToMerge, err := getPartitions(tableCtx, db, dbName, tableName)
		if err != nil {
			res.err = fmt.Errorf("error getting partitions to process: %w", err)
			return res
		}
		if len(partitionsToMerge) == 0 {
			res.skipped = true
		} else {
			for id, name := range partitionsToMerge {
				wg.Add(1)
				go mergePartitionWorker(tableCtx, &wg, db, dbName, tableName, name, id, splitSizeMB)
			}
		}
	} else {
		log.Info("table is not partitioned", zap.String("db", dbName), zap.String("table", tableName))
		if beforeSummary[0].regionCount <= 1 {
			res.skipped = true
		} else {
			tableID, err := getTableInfo(tableCtx, db, dbName, tableName)
			if err != nil {
				res.err = fmt.Errorf("failed to get table info: %w", err)
				return res
			}
			wg.Add(1)
			go mergePartitionWorker(tableCtx, &wg, db, dbName, tableName, tableName, tableID, splitSizeMB)
		}
	}
	wg.Wait()
	res.elapsed = time.Since(start)

	if tableCtx.Err() == context.DeadlineExceeded {
		log.Error("table processing timed out", zap.String("table", tableName), zap.Duration("timeout", *tableTimeout))
		res.err = context.DeadlineExceeded
	} else if tableCtx.Err() != nil {
		res.err = tableCtx.Err()
	}

	afterSummary, err := getPartitionSummary(context.Background(), db, dbName, tableName)
	if err != nil {
		log.Warn("failed to get 'after' summary", zap.String("table", tableName), zap.Error(err))
	}

	afterMap := make(map[string]partitionInfo)
	for _, s := range afterSummary {
		afterMap[s.name] = s
	}
	for _, s := range beforeSummary {
		afterP, ok := afterMap[s.name]
		if !ok {
			afterP = s
		}

		res.partitions = append(res.partitions, partitionResult{
			partitionName:  s.name,
			beforeCount:    s.regionCount,
			afterCount:     afterP.regionCount,
			avgSizeMBAfter: afterP.avgSizeMB,
		})
	}

	return res
}

func main() {
	flag.Parse()
	if *allTables && *tableName != "" {
		log.Fatal("cannot specify both -table and -all-tables flags")
	}
	if !*allTables && *tableName == "" {
		log.Fatal("a required flag is not provided: use -table or -all-tables")
	}

	client = newPDHttpClient()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Warn("cancellation signal received, stopping workers gracefully...")
		cancel()
	}()

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		log.Fatal("failed to open database connection", zap.Error(err))
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		if ctx.Err() != nil {
			log.Warn("database ping cancelled.")
			return
		}
		log.Fatal("failed to connect to database", zap.Error(err))
	}

	splitSizeMB, err := getSplitSizeMB(ctx, db)
	if err != nil {
		log.Fatal("could not retrieve TiKV split size configuration", zap.Error(err))
	}

	var results []tableResult
	overallStart := time.Now()

	if *allTables {
		tables, err := getAllTables(ctx, db, *dbName)
		if err != nil {
			if ctx.Err() != nil {
				log.Warn("getting table list was cancelled.")
				return
			}
			log.Fatal("failed to get list of all tables", zap.Error(err))
		}
		log.Info("starting to process all tables in database", zap.String("db", *dbName), zap.Int("count", len(tables)))

		var wg sync.WaitGroup
		resultsChan := make(chan tableResult, len(tables))

		for _, tbl := range tables {
			wg.Add(1)
			go func(t string) {
				defer wg.Done()
				resultsChan <- processTable(ctx, db, *dbName, t, splitSizeMB)
			}(tbl)
		}

		wg.Wait()
		close(resultsChan)

		for res := range resultsChan {
			results = append(results, res)
		}

	} else {
		results = append(results, processTable(ctx, db, *dbName, *tableName, splitSizeMB))
	}

	log.Info("all processing tasks are complete", zap.Duration("total elapsed", time.Since(overallStart)))
	printUnifiedSummary(results)
}
