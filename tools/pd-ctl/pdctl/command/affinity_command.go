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
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/pingcap/errors"

	pd "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/tools/pd-ctl/helper/tidb/codec"
)

const (
	tidbTLSConfigName       = "pdctl-tidb"
	tableGroupIDPattern     = "_tidb_c_t_%d"
	partitionGroupIDPattern = "_tidb_p_t_%d_p%d"
)

var (
	partitionGroupRegexp = regexp.MustCompile(`^_tidb_p_t_(\d+)_p(\d+)$`)
	tableGroupRegexp     = regexp.MustCompile(`^_tidb_c_t_(\d+)$`)
)

type partitionInfo struct {
	ID   int64
	Name string
}

type tableAffinityInfo struct {
	DB         string
	Table      string
	TableID    int64
	Partitions []partitionInfo
}

type affinityGroupDefinition struct {
	id     string
	ranges []pd.AffinityGroupKeyRange
}

type affinityGroupResolved struct {
	GroupID     string                 `json:"group_id"`
	Database    string                 `json:"database,omitempty"`
	Table       string                 `json:"table,omitempty"`
	Partition   string                 `json:"partition,omitempty"`
	TableID     int64                  `json:"table_id"`
	PartitionID int64                  `json:"partition_id,omitempty"`
	State       *pd.AffinityGroupState `json:"state,omitempty"`
}

// NewAffinityCommand creates the affinity command.
func NewAffinityCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "affinity",
		Short:             "affinity group commands based on TiDB metadata",
		PersistentPreRunE: requirePDClient,
	}
	cmd.PersistentFlags().String("dsn", "", "TiDB DSN used to fetch table/partition information")
	cmd.PersistentFlags().String("db", "", "database name of the target table")
	cmd.PersistentFlags().String("table", "", "table name of the target table")
	cmd.PersistentFlags().String("partition", "", "target partition name or ID when operating on a partitioned table")

	cmd.AddCommand(
		newAffinityCreateCommand(),
		newAffinityGetCommand(),
		newAffinityDeleteCommand(),
		newAffinityUpdatePeersCommand(),
		newAffinityListCommand(),
	)
	return cmd
}

func newAffinityCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "create affinity groups for a table or its partitions",
		Run:   affinityCreateCommandFunc,
	}
	return cmd
}

func newAffinityGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get",
		Short: "get affinity group states for the target table",
		Run:   affinityGetCommandFunc,
	}
	return cmd
}

func newAffinityDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "delete affinity groups for the target table",
		Run:   affinityDeleteCommandFunc,
	}
	cmd.Flags().Bool("force", false, "force delete the affinity group even if it has key ranges")
	return cmd
}

func newAffinityUpdatePeersCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update",
		Short: "update leader and voters for a specific affinity group",
		Run:   affinityUpdatePeersCommandFunc,
	}
	cmd.Flags().Uint64("leader", 0, "leader store ID")
	cmd.Flags().String("voters", "", "comma separated voter store IDs, e.g. 1,2,3")
	return cmd
}

func newAffinityListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "list affinity groups with resolved table and partition names",
		Run:   affinityListCommandFunc,
	}
	cmd.Flags().String("dsn", "", "TiDB DSN used to resolve table names")
	return cmd
}

func affinityCreateCommandFunc(cmd *cobra.Command, _ []string) {
	info, partition, err := loadTableAffinityInfo(cmd)
	if err != nil {
		cmd.Println(err)
		return
	}
	defs, err := buildAffinityGroupDefinitions(info, partition)
	if err != nil {
		cmd.Println(err)
		return
	}
	groups := make(map[string][]pd.AffinityGroupKeyRange, len(defs))
	for _, def := range defs {
		groups[def.id] = def.ranges
	}
	resp, err := PDCli.CreateAffinityGroups(cmd.Context(), groups)
	if err != nil {
		cmd.Printf("Failed to create affinity groups: %v\n", err)
		return
	}
	jsonPrint(cmd, resp)
}

func affinityGetCommandFunc(cmd *cobra.Command, _ []string) {
	info, partition, err := loadTableAffinityInfo(cmd)
	if err != nil {
		cmd.Println(err)
		return
	}
	defs, err := buildAffinityGroupDefinitions(info, partition)
	if err != nil {
		cmd.Println(err)
		return
	}
	result := make(map[string]*pd.AffinityGroupState, len(defs))
	found := false
	for _, def := range defs {
		state, err := PDCli.GetAffinityGroup(cmd.Context(), def.id)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "404") {
				continue
			}
			cmd.Printf("Failed to get affinity group %s: %v\n", def.id, err)
			return
		}
		result[def.id] = state
		found = true
	}
	if !found {
		cmd.Println("No affinity groups found")
		return
	}
	jsonPrint(cmd, result)
}

func affinityDeleteCommandFunc(cmd *cobra.Command, _ []string) {
	info, partition, err := loadTableAffinityInfo(cmd)
	if err != nil {
		cmd.Println(err)
		return
	}
	defs, err := buildAffinityGroupDefinitions(info, partition)
	if err != nil {
		cmd.Println(err)
		return
	}
	force, _ := cmd.Flags().GetBool("force")
	ids := collectGroupIDs(defs)
	if len(ids) == 1 {
		if err := PDCli.DeleteAffinityGroup(cmd.Context(), ids[0], force); err != nil {
			cmd.Printf("Failed to delete affinity group %s: %v\n", ids[0], err)
			return
		}
		cmd.Printf("Affinity group %s deleted\n", ids[0])
		return
	}
	if err := PDCli.BatchDeleteAffinityGroups(cmd.Context(), ids, force); err != nil {
		cmd.Printf("Failed to batch delete affinity groups: %v\n", err)
		return
	}
	cmd.Printf("Affinity groups deleted: %s\n", strings.Join(ids, ","))
}

func affinityUpdatePeersCommandFunc(cmd *cobra.Command, _ []string) {
	info, partition, err := loadTableAffinityInfo(cmd)
	if err != nil {
		cmd.Println(err)
		return
	}
	defs, err := buildAffinityGroupDefinitions(info, partition)
	if err != nil {
		cmd.Println(err)
		return
	}
	// TODO: support batch updating all partitions in one call when needed.
	if len(defs) != 1 {
		cmd.Println("Specify --partition to target a single affinity group in partitioned tables")
		return
	}
	leader, _ := cmd.Flags().GetUint64("leader")
	if leader == 0 {
		cmd.Println("leader is required")
		return
	}
	voterStr, _ := cmd.Flags().GetString("voters")
	voters, err := parseUint64List(voterStr)
	if err != nil {
		cmd.Printf("Failed to parse voters: %v\n", err)
		return
	}
	if len(voters) == 0 {
		cmd.Println("voters is required")
		return
	}
	state, err := PDCli.UpdateAffinityGroupPeers(cmd.Context(), defs[0].id, leader, voters)
	if err != nil {
		cmd.Printf("Failed to update affinity group peers: %v\n", err)
		return
	}
	jsonPrint(cmd, state)
}

func loadTableAffinityInfo(cmd *cobra.Command) (tableAffinityInfo, string, error) {
	dbName, _ := cmd.Flags().GetString("db")
	tableName, _ := cmd.Flags().GetString("table")
	dsn, _ := cmd.Flags().GetString("dsn")
	partition, _ := cmd.Flags().GetString("partition")
	if dbName == "" || tableName == "" {
		return tableAffinityInfo{}, "", errors.New("db and table are required")
	}
	if dsn == "" {
		return tableAffinityInfo{}, "", errors.New("dsn is required to fetch table metadata from TiDB")
	}
	info, err := fetchTableAffinityInfo(cmd.Context(), cmd, dsn, dbName, tableName)
	if err != nil {
		return tableAffinityInfo{}, "", err
	}
	return info, partition, nil
}

func fetchTableAffinityInfo(ctx context.Context, cmd *cobra.Command, dsn, dbName, tableName string) (tableAffinityInfo, error) {
	cfg, err := buildTiDBConfig(cmd, dsn)
	if err != nil {
		return tableAffinityInfo{}, err
	}
	db, err := openTiDBWithConfig(cfg)
	if err != nil {
		return tableAffinityInfo{}, errors.WithStack(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		return tableAffinityInfo{}, errors.WithStack(err)
	}
	var tableID int64
	query := `SELECT TIDB_TABLE_ID FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`
	if err := db.QueryRowContext(ctx, query, dbName, tableName).Scan(&tableID); err != nil {
		if errors.ErrorEqual(err, sql.ErrNoRows) {
			return tableAffinityInfo{}, errors.Errorf("table %s.%s not found", dbName, tableName)
		}
		return tableAffinityInfo{}, errors.WithStack(err)
	}
	partitions, err := fetchPartitionInfo(ctx, db, dbName, tableName)
	if err != nil {
		return tableAffinityInfo{}, err
	}
	return tableAffinityInfo{
		DB:         dbName,
		Table:      tableName,
		TableID:    tableID,
		Partitions: partitions,
	}, nil
}

func fetchPartitionInfo(ctx context.Context, db *sql.DB, dbName, tableName string) ([]partitionInfo, error) {
	query := `SELECT PARTITION_NAME, TIDB_PARTITION_ID FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL ORDER BY TIDB_PARTITION_ID`
	rows, err := db.QueryContext(ctx, query, dbName, tableName)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer rows.Close()
	var partitions []partitionInfo
	for rows.Next() {
		var name string
		var id int64
		if err := rows.Scan(&name, &id); err != nil {
			return nil, errors.WithStack(err)
		}
		partitions = append(partitions, partitionInfo{
			ID:   id,
			Name: name,
		})
	}
	return partitions, nil
}

func ensureTiDBTLS(cmd *cobra.Command, cfg *mysql.Config) error {
	tlsConfig, err := parseTLSConfig(cmd)
	if err != nil || tlsConfig == nil {
		return err
	}
	if err := mysql.RegisterTLSConfig(tidbTLSConfigName, tlsConfig); err != nil && !strings.Contains(err.Error(), "duplicate") {
		return errors.WithStack(err)
	}
	cfg.Params["tls"] = tidbTLSConfigName
	return nil
}

func buildTiDBConfig(cmd *cobra.Command, dsn string) (*mysql.Config, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	if _, ok := cfg.Params["tls"]; !ok {
		if err := ensureTiDBTLS(cmd, cfg); err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

func openTiDB(cmd *cobra.Command, dsn string) (*sql.DB, error) {
	cfg, err := buildTiDBConfig(cmd, dsn)
	if err != nil {
		return nil, err
	}
	return openTiDBWithConfig(cfg)
}

func openTiDBWithConfig(cfg *mysql.Config) (*sql.DB, error) {
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}

func buildAffinityGroupDefinitions(info tableAffinityInfo, partition string) ([]affinityGroupDefinition, error) {
	if len(info.Partitions) == 0 {
		if partition != "" {
			return nil, errors.New("--partition is only allowed for partitioned tables")
		}
		ranges := buildKeyRanges(info, info.Partitions)
		return []affinityGroupDefinition{{
			id:     tableGroupID(info.TableID),
			ranges: ranges,
		}}, nil
	}
	selected := info.Partitions
	if partition != "" {
		match, err := selectPartition(info.Partitions, partition)
		if err != nil {
			return nil, err
		}
		selected = []partitionInfo{match}
	}
	defs := make([]affinityGroupDefinition, 0, len(selected))
	for _, p := range selected {
		start, end := tableKeyRange(p.ID)
		defs = append(defs, affinityGroupDefinition{
			id: partitionGroupID(info.TableID, p.ID),
			ranges: []pd.AffinityGroupKeyRange{{
				StartKey: start,
				EndKey:   end,
			}},
		})
	}
	return defs, nil
}

func buildKeyRanges(info tableAffinityInfo, partitions []partitionInfo) []pd.AffinityGroupKeyRange {
	if len(partitions) == 0 {
		start, end := tableKeyRange(info.TableID)
		return []pd.AffinityGroupKeyRange{{StartKey: start, EndKey: end}}
	}
	ranges := make([]pd.AffinityGroupKeyRange, 0, len(partitions))
	for _, p := range partitions {
		start, end := tableKeyRange(p.ID)
		ranges = append(ranges, pd.AffinityGroupKeyRange{StartKey: start, EndKey: end})
	}
	return ranges
}

func selectPartition(partitions []partitionInfo, target string) (partitionInfo, error) {
	for _, p := range partitions {
		if strings.EqualFold(p.Name, target) || strconv.FormatInt(p.ID, 10) == target {
			return p, nil
		}
	}
	return partitionInfo{}, errors.Errorf("partition %s not found", target)
}

func collectGroupIDs(defs []affinityGroupDefinition) []string {
	ids := make([]string, 0, len(defs))
	for _, def := range defs {
		ids = append(ids, def.id)
	}
	return ids
}

func tableKeyRange(id int64) ([]byte, []byte) {
	return encodeTablePrefix(id), encodeTablePrefix(id + 1)
}

func encodeTablePrefix(id int64) []byte {
	key := make([]byte, 0, 9)
	key = append(key, 't')
	return codec.EncodeInt(key, id)
}

func partitionGroupID(tableID, partitionID int64) string {
	return fmt.Sprintf(partitionGroupIDPattern, tableID, partitionID)
}

func tableGroupID(tableID int64) string {
	return fmt.Sprintf(tableGroupIDPattern, tableID)
}

func parseUint64List(input string) ([]uint64, error) {
	if strings.TrimSpace(input) == "" {
		return nil, nil
	}
	parts := strings.Split(input, ",")
	res := make([]uint64, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		v, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		res = append(res, v)
	}
	return res, nil
}

func affinityListCommandFunc(cmd *cobra.Command, _ []string) {
	dsn, _ := cmd.Flags().GetString("dsn")
	if dsn == "" {
		cmd.Println("dsn is required to resolve table names")
		return
	}
	groups, err := PDCli.GetAllAffinityGroups(cmd.Context())
	if err != nil {
		cmd.Printf("Failed to get affinity groups: %v\n", err)
		return
	}

	parsed := parseGroupIDs(groups)
	if len(parsed.tableIDs) == 0 && len(parsed.partitionIDs) == 0 {
		jsonPrint(cmd, []affinityGroupResolved{})
		return
	}

	db, err := openTiDB(cmd, dsn)
	if err != nil {
		cmd.Println(err)
		return
	}
	defer db.Close()

	if err := db.PingContext(cmd.Context()); err != nil {
		cmd.Println(errors.WithStack(err))
		return
	}

	tableNames, _ := fetchTablesByIDs(cmd.Context(), db, parsed.tableIDs)
	partitionNames, _ := fetchPartitionsByIDs(cmd.Context(), db, parsed.partitionIDs)

	result := make([]affinityGroupResolved, 0, len(groups))
	for id, state := range groups {
		if info, ok := parsed.partitionGroups[id]; ok {
			pName := partitionNames[info.partitionID]
			tName := tableNames[pName.TableID]
			result = append(result, affinityGroupResolved{
				GroupID:     id,
				Database:    tName.Schema,
				Table:       tName.Name,
				Partition:   pName.Name,
				TableID:     info.tableID,
				PartitionID: info.partitionID,
				State:       state,
			})
			continue
		}
		if info, ok := parsed.tableGroups[id]; ok {
			tName := tableNames[info.tableID]
			result = append(result, affinityGroupResolved{
				GroupID:  id,
				Database: tName.Schema,
				Table:    tName.Name,
				TableID:  info.tableID,
				State:    state,
			})
		}
	}
	jsonPrint(cmd, result)
}

type parsedGroups struct {
	tableIDs        []int64
	partitionIDs    []int64
	partitionGroups map[string]struct {
		tableID     int64
		partitionID int64
	}
	tableGroups map[string]struct {
		tableID int64
	}
}

func parseGroupIDs(groups map[string]*pd.AffinityGroupState) parsedGroups {
	pg := parsedGroups{
		partitionGroups: make(map[string]struct {
			tableID     int64
			partitionID int64
		}),
		tableGroups: make(map[string]struct{ tableID int64 }),
	}
	for id := range groups {
		if matches := partitionGroupRegexp.FindStringSubmatch(id); len(matches) == 3 {
			tableID, _ := strconv.ParseInt(matches[1], 10, 64)
			partitionID, _ := strconv.ParseInt(matches[2], 10, 64)
			pg.partitionGroups[id] = struct {
				tableID     int64
				partitionID int64
			}{tableID: tableID, partitionID: partitionID}
			pg.tableIDs = append(pg.tableIDs, tableID)
			pg.partitionIDs = append(pg.partitionIDs, partitionID)
			continue
		}
		if matches := tableGroupRegexp.FindStringSubmatch(id); len(matches) == 2 {
			tableID, _ := strconv.ParseInt(matches[1], 10, 64)
			pg.tableGroups[id] = struct{ tableID int64 }{tableID: tableID}
			pg.tableIDs = append(pg.tableIDs, tableID)
		}
	}
	pg.tableIDs = uniqInt64(pg.tableIDs)
	pg.partitionIDs = uniqInt64(pg.partitionIDs)
	return pg
}

func uniqInt64(ids []int64) []int64 {
	if len(ids) == 0 {
		return ids
	}
	m := make(map[int64]struct{}, len(ids))
	res := make([]int64, 0, len(ids))
	for _, id := range ids {
		if _, ok := m[id]; ok {
			continue
		}
		m[id] = struct{}{}
		res = append(res, id)
	}
	return res
}

type tableName struct {
	Schema string
	Name   string
}

type partitionName struct {
	TableID int64
	Name    string
}

func fetchTablesByIDs(ctx context.Context, db *sql.DB, ids []int64) (map[int64]tableName, error) {
	res := make(map[int64]tableName, len(ids))
	if len(ids) == 0 {
		return res, nil
	}
	if err := queryTablesByIDs(ctx, db, ids, "TABLE_ID", res); err != nil {
		return nil, err
	}
	return res, nil
}

func fetchPartitionsByIDs(ctx context.Context, db *sql.DB, ids []int64) (map[int64]partitionName, error) {
	res := make(map[int64]partitionName, len(ids))
	if len(ids) == 0 {
		return res, nil
	}
	if err := queryPartitionsByIDs(ctx, db, ids, "PARTITION_ID", "TABLE_ID", res); err != nil {
		return nil, err
	}
	return res, nil
}

func queryTablesByIDs(ctx context.Context, db *sql.DB, ids []int64, col string, res map[int64]tableName) error {
	placeholders := strings.Repeat("?,", len(ids))
	placeholders = strings.TrimRight(placeholders, ",")
	query := fmt.Sprintf(`SELECT %s, TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE %s IN (%s)`, col, col, placeholders)
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return errors.WithStack(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var schema, name string
		if err := rows.Scan(&id, &schema, &name); err != nil {
			return errors.WithStack(err)
		}
		res[id] = tableName{Schema: schema, Name: name}
	}
	return nil
}

func queryPartitionsByIDs(ctx context.Context, db *sql.DB, ids []int64, partCol, tableCol string, res map[int64]partitionName) error {
	placeholders := strings.Repeat("?,", len(ids))
	placeholders = strings.TrimRight(placeholders, ",")
	query := fmt.Sprintf(`SELECT %s, %s, PARTITION_NAME FROM INFORMATION_SCHEMA.PARTITIONS WHERE %s IN (%s)`, partCol, tableCol, partCol, placeholders)
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return errors.WithStack(err)
	}
	defer rows.Close()
	for rows.Next() {
		var pid, tableID int64
		var name string
		if err := rows.Scan(&pid, &tableID, &name); err != nil {
			return errors.WithStack(err)
		}
		res[pid] = partitionName{TableID: tableID, Name: name}
	}
	return nil
}
