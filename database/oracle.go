/*
Copyright © 2020 Marvin

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
package database

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/godror/godror"
	"github.com/godror/godror/dsn"
	"github.com/greatcloak/decimal"
	"github.com/wentaojin/scan/common"
	"github.com/wentaojin/scan/config"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"time"
)

type Oracle struct {
	Ctx      context.Context
	OracleDB *sql.DB
}

// 创建 oracle 数据库引擎
func NewOracleDBEngine(ctx context.Context, oraCfg config.OracleConfig) (*Oracle, error) {
	// https://pkg.go.dev/github.com/godror/godror
	// https://github.com/godror/godror/blob/db9cd12d89cdc1c60758aa3f36ece36cf5a61814/doc/connection.md
	// https://godror.github.io/godror/doc/connection.html
	// You can specify connection timeout seconds with "?connect_timeout=15" - Ping uses this timeout, NOT the Deadline in Context!
	// For more connection options, see [Godor Connection Handling](https://godror.github.io/godror/doc/connection.html).
	var (
		connString string
		oraDSN     dsn.ConnectionParams
		err        error
	)

	//https://www.syntio.net/en/labs-musings/efficient-fetching-of-data-from-oracle-database-in-golang/
	// https://github.com/godror/godror/pull/65
	//connClass := fmt.Sprintf("pool_%v", xid.New().String())
	//connString = fmt.Sprintf("oracle://@%s/%s?connectionClass=%s&%s",
	//	StringsBuilder(oraCfg.Host, ":", strconv.Itoa(oraCfg.Port)),
	//	oraCfg.ServiceName, "connClass", oraCfg.ConnectParams)
	connString = fmt.Sprintf("oracle://@%s/%s?standaloneConnection=1&%s",
		fmt.Sprintf("%s:%s", oraCfg.Host, strconv.Itoa(oraCfg.Port)),
		oraCfg.ServiceName, oraCfg.ConnectParams)
	oraDSN, err = godror.ParseDSN(connString)
	if err != nil {
		return nil, err
	}

	oraDSN.Username, oraDSN.Password = oraCfg.Username, godror.NewPassword(oraCfg.Password)

	if !strings.EqualFold(oraCfg.PDBName, "") {
		oraCfg.SessionParams = append(oraCfg.SessionParams, fmt.Sprintf(`ALTER SESSION SET CONTAINER = %s`, oraCfg.PDBName))
	}

	if !strings.EqualFold(oraCfg.Username, oraCfg.Schema) && !strings.EqualFold(oraCfg.Schema, "") {
		oraCfg.SessionParams = append(oraCfg.SessionParams, fmt.Sprintf(`ALTER SESSION SET CURRENT_SCHEMA = %s`, strings.ToUpper(oraCfg.Schema)))
	}

	// 关闭外部认证
	oraDSN.ExternalAuth = false
	oraDSN.OnInitStmts = oraCfg.SessionParams

	// charset 字符集
	if !strings.EqualFold(oraCfg.Charset, "") {
		oraDSN.CommonParams.Charset = oraCfg.Charset
	}

	// godror logger 日志输出
	// godror.SetLogger(zapr.NewLogger(zap.L()))

	sqlDB := sql.OpenDB(godror.NewConnector(oraDSN))
	sqlDB.SetMaxIdleConns(0)
	sqlDB.SetMaxOpenConns(0)
	sqlDB.SetConnMaxLifetime(0)

	err = sqlDB.Ping()
	if err != nil {
		return nil, fmt.Errorf("error on ping oracle database connection:%v", err)
	}
	return &Oracle{
		Ctx:      ctx,
		OracleDB: sqlDB,
	}, nil
}

func (o *Oracle) StartOracleChunkCreateTask(taskName string) error {
	querySQL := common.StringsBuilder(`SELECT COUNT(1) COUNT FROM dba_parallel_execute_chunks WHERE TASK_NAME='`, taskName, `'`)
	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return err
	}
	if res[0]["COUNT"] != "0" {
		if err = o.CloseOracleChunkTask(taskName); err != nil {
			return err
		}
	}

	createSQL := common.StringsBuilder(`BEGIN
  DBMS_PARALLEL_EXECUTE.CREATE_TASK (task_name => '`, taskName, `');
END;`)
	_, err = o.OracleDB.ExecContext(o.Ctx, createSQL)
	if err != nil {
		return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE create task failed: %v, sql: %v", err, createSQL)
	}
	return nil
}

func (o *Oracle) StartOracleCreateChunkByRowID(taskName, schemaName, tableName string, chunkSize string, callTimeout int64) error {
	deadline := time.Now().Add(time.Duration(callTimeout) * time.Second)

	zap.L().Warn("current calltimeout",
		zap.Int64("calltimeout", callTimeout),
		zap.String("deadline", deadline.String()))
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	chunkSQL := common.StringsBuilder(`BEGIN
  DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID (task_name   => '`, taskName, `',
                                               table_owner => '`, schemaName, `',
                                               table_name  => '`, tableName, `',
                                               by_row      => TRUE,
                                               chunk_size  => `, chunkSize, `);
END;`)
	_, err := o.OracleDB.ExecContext(ctx, chunkSQL)
	if err != nil {
		return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE create_chunks_by_rowid task failed: %v, sql: %v", err, chunkSQL)
	}

	return nil
}

func (o *Oracle) GetOracleTableChunksByRowID(taskName string) ([]map[string]string, error) {
	querySQL := common.StringsBuilder(`SELECT 'ROWID BETWEEN ''' || start_rowid || ''' AND ''' || end_rowid || '''' CMD FROM dba_parallel_execute_chunks WHERE  task_name = '`, taskName, `' ORDER BY chunk_id`)

	_, res, err := Query(o.Ctx, o.OracleDB, querySQL)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (o *Oracle) CloseOracleChunkTask(taskName string) error {
	clearSQL := common.StringsBuilder(`BEGIN
  DBMS_PARALLEL_EXECUTE.DROP_TASK ('`, taskName, `');
END;`)

	_, err := o.OracleDB.ExecContext(o.Ctx, clearSQL)
	if err != nil {
		return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE drop task failed: %v, sql: %v", err, clearSQL)
	}

	return nil
}

func (o *Oracle) GetOracleSchemaTable(schemaName string) ([]string, error) {
	var (
		tables []string
		err    error
	)
	_, res, err := Query(o.Ctx, o.OracleDB, fmt.Sprintf(`SELECT table_name AS TABLE_NAME FROM DBA_TABLES WHERE UPPER(owner) = UPPER('%s') AND (IOT_TYPE IS NUll OR IOT_TYPE='IOT')`, schemaName))
	if err != nil {
		return tables, err
	}
	for _, r := range res {
		tables = append(tables, strings.ToUpper(r["TABLE_NAME"]))
	}

	return tables, nil
}

func (o *Oracle) ScanOracleTableDecimalData(m Full, sourceDBCharset, targetDBCharset string, bigintStr, unsinBigintStr decimal.Decimal) ([]Scan, error) {
	var (
		err         error
		columnNames []string
		columnTypes []string
		sqlStr      string

		results []Scan
	)

	columnDetail := m.ColumnDetailT
	convertUtf8Raw, err := common.CharsetConvert([]byte(columnDetail), targetDBCharset, common.CharsetUTF8MB4)
	if err != nil {
		return results, fmt.Errorf("column [%s] charset convert failed, %v", columnDetail, err)
	}

	convertTargetRaw, err := common.CharsetConvert(convertUtf8Raw, common.CharsetUTF8MB4, sourceDBCharset)
	if err != nil {
		return results, fmt.Errorf("column [%s] charset convert failed, %v", columnDetail, err)
	}
	columnDetail = string(convertTargetRaw)

	if strings.EqualFold(m.SQLHint, "") {
		sqlStr = fmt.Sprintf("SELECT %v FROM %s.%s WHERE %v", columnDetail, m.SchemaNameT, m.TableNameT, m.ChunkDetailT)
	} else {
		sqlStr = fmt.Sprintf("SELECT %v %v FROM %s.%s WHERE %v", m.SQLHint, columnDetail, m.SchemaNameT, m.TableNameT, m.ChunkDetailT)
	}

	rows, err := o.OracleDB.QueryContext(o.Ctx, sqlStr)
	if err != nil {
		return results, err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return results, fmt.Errorf("failed to csv get rows columnTypes: %v", err)
	}

	for _, ct := range colTypes {
		convertUtf8Raw, err = common.CharsetConvert([]byte(ct.Name()), sourceDBCharset, common.CharsetUTF8MB4)
		if err != nil {
			return results, fmt.Errorf("column [%s] charset convert failed, %v", ct.Name(), err)
		}

		convertTargetRaw, err = common.CharsetConvert(convertUtf8Raw, common.CharsetUTF8MB4, targetDBCharset)
		if err != nil {
			return results, fmt.Errorf("column [%s] charset convert failed, %v", ct.Name(), err)
		}
		columnNames = append(columnNames, string(convertTargetRaw))
		columnTypes = append(columnTypes, ct.ScanType().String())
	}

	// 数据 SCAN
	columnNums := len(columnNames)
	rawResult := make([][]byte, columnNums)
	dest := make([]interface{}, columnNums)
	for i := range rawResult {
		dest[i] = &rawResult[i]
	}

	// 表行数读取
	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			return results, err
		}

		var (
			rowid   string
			columns []*Column
		)

		for i, raw := range rawResult {
			switch columnTypes[i] {
			case "godror.Number":
				if raw == nil {
					columns = append(columns, &Column{
						ColumnName:           columnNames[i],
						ColumnValue:          fmt.Sprintf("%v", `NULL`),
						ColumnBigint:         "UNKNOWN",
						ColumnUnsingedBigint: "UNKNOWN",
					})
				} else if string(raw) == "" {
					columns = append(columns, &Column{
						ColumnName:           columnNames[i],
						ColumnValue:          fmt.Sprintf("%v", `NULL`),
						ColumnBigint:         "UNKNOWN",
						ColumnUnsingedBigint: "UNKNOWN",
					})
				} else {
					decimalStr, err := decimal.NewFromString(string(raw))
					if err != nil {
						return results, err
					}

					cmpBigint := decimalStr.Cmp(bigintStr)
					// > BIGINT
					if cmpBigint == 1 {
						cmpUnsGigint := decimalStr.Cmp(unsinBigintStr)

						// > UNSIGNED BIGINT
						if cmpUnsGigint == 1 {
							columns = append(columns, &Column{
								ColumnName:           columnNames[i],
								ColumnValue:          string(raw),
								ColumnBigint:         fmt.Sprintf("> %s", bigintStr),
								ColumnUnsingedBigint: fmt.Sprintf("> %s", unsinBigintStr),
							})
						} else {
							// <= UNSIGNED BIGINT
							columns = append(columns, &Column{
								ColumnName:           columnNames[i],
								ColumnValue:          string(raw),
								ColumnBigint:         fmt.Sprintf("> %s", bigintStr),
								ColumnUnsingedBigint: fmt.Sprintf("<= %s", unsinBigintStr),
							})
						}
					}
				}
			default:
				if strings.EqualFold(columnNames[i], "ROWID") {
					rowid = fmt.Sprintf("%v", string(raw))
				} else {
					return results, fmt.Errorf("sql [%v] query meet panic data, column [%v] columntype [%v] columnvalue [%v]", sqlStr, columnNames[i], columnTypes[i], string(raw))
				}
			}
		}

		if len(columns) > 0 {
			for _, c := range columns {
				results = append(results, Scan{
					SchemaNameT:   m.SchemaNameT,
					TableNameT:    m.TableNameT,
					SQLHint:       m.SQLHint,
					ColumnDetailT: m.ColumnDetailT,
					ChunkDetailT:  m.ChunkDetailT,
					RowID:         rowid,
					Column:        c,
				})
			}
		}
	}

	if err = rows.Err(); err != nil {
		return results, err
	}

	return results, nil
}
