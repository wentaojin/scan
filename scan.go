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
package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/greatcloak/decimal"
	"github.com/wentaojin/scan/common"
	"github.com/wentaojin/scan/config"
	"github.com/wentaojin/scan/database"
	"github.com/wentaojin/scan/logger"
	"github.com/wentaojin/scan/signal"
	"github.com/xxjwxc/gowp/workpool"
	"go.uber.org/zap"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	cfg := config.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		log.Fatalf("start meta failed. error is [%s], Use '--help' for help.", err)
	}

	logger.NewZapLogger(cfg)

	signal.SetupSignalHandler(func() {
		os.Exit(1)
	})
	ctx := context.Background()
	if err := Run(ctx, cfg); err != nil {
		zap.L().Fatal("server run failed", zap.Error(err))
	}
}

func Run(ctx context.Context, cfg *config.Config) error {
	sTime := time.Now()

	zap.L().Info("welcome to scan program", zap.String("config", cfg.String()))

	mysqldb, err := database.NewMySQLDBEngine(ctx, cfg.MySQLConfig)
	if err != nil {
		return err
	}
	metaDB, err := database.NewMetaDBEngine(ctx, cfg.MetaConfig)
	if err != nil {
		return err
	}
	oracleDB, err := database.NewOracleDBEngine(ctx, cfg.OracleConfig)
	if err != nil {
		return err
	}
	zap.L().Info("create database connect success", zap.String("cost", time.Now().Sub(sTime).String()))

	err = Init(ctx, metaDB, mysqldb, cfg)
	if err != nil {
		return err
	}

	tables, err := database.NewWaitModel(metaDB).DetailWaitSyncMeta(ctx, &database.Wait{
		SchemaNameT: strings.ToUpper(cfg.OracleConfig.Schema),
	})
	if err != nil {
		return err
	}

	err = Split(ctx, metaDB, oracleDB, cfg, tables)
	if err != nil {
		return err
	}

	err = Scan(ctx, metaDB, oracleDB, cfg, tables)
	if err != nil {
		return err
	}

	err = Statistics(ctx, metaDB, mysqldb, cfg, tables)
	if err != nil {
		return err
	}
	zap.L().Info("scan database program finished", zap.String("cost", time.Now().Sub(sTime).String()))

	return nil
}

func Init(ctx context.Context, dbM *database.Meta, dbS *database.MySQL, cfg *config.Config) error {
	sTime := time.Now()
	err := dbM.MigrateTables()
	if err != nil {
		return err
	}
	zap.L().Info("migrate meta tables success", zap.String("cost", time.Now().Sub(sTime).String()))

	tTime := time.Now()
	tables, err := dbS.GetMySQLTables(cfg.MySQLConfig.Schema)
	if err != nil {
		return err
	}
	zap.L().Info("get mysql database all tables success", zap.String("cost", time.Now().Sub(tTime).String()))

	g := workpool.New(cfg.AppConfig.SQLThread)

	for _, tab := range tables {
		t := tab
		g.Do(func() error {
			mTime := time.Now()
			zap.L().Info("get mysql database decimal single table starting", zap.String("schema", strings.ToUpper(cfg.OracleConfig.Schema)), zap.String("table", strings.ToUpper(t)), zap.String("startTime", mTime.String()))

			columns, err := dbS.GetMySQLTableColumn(cfg.MySQLConfig.Schema, t)
			if err != nil {
				return err
			}

			var column []string

			for _, c := range columns {
				if strings.EqualFold(c["DATA_TYPE"], "DECIMAL") {
					precision, err := strconv.Atoi(c["DATA_PRECISION"])
					if err != nil {
						return err
					}
					scale, err := strconv.Atoi(c["DATA_SCALE"])
					if err != nil {
						return err
					}
					switch scale {
					case 0:
						// bigint <= decimal(19,0)
						if precision >= 19 {
							column = append(column, c["COLUMN_NAME"])
						}
					default:
						zap.L().Warn("current table decimal single data_scale",
							zap.String("schema", cfg.MySQLConfig.Schema),
							zap.String("table", t),
							zap.String("datatype", fmt.Sprintf("decimal(%d,%d)", precision, scale)))
					}
				}
			}

			if len(column) > 0 {
				column = append(column, "ROWID")
				err = database.NewWaitModel(dbM).CreateWaitSyncMeta(ctx, &database.Wait{
					SchemaNameT:   strings.ToUpper(cfg.OracleConfig.Schema),
					TableNameS:    strings.ToUpper(t),
					ColumnDetailS: strings.Join(column, ","),
				})
				if err != nil {
					return err
				}
			}

			zap.L().Info("get mysql database decimal single table success", zap.String("schema", strings.ToUpper(cfg.OracleConfig.Schema)), zap.String("table", strings.ToUpper(t)), zap.String("cost", time.Now().Sub(mTime).String()))
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	zap.L().Info("init mysql database decimal tables task success", zap.String("cost", time.Now().Sub(sTime).String()))
	return nil
}

func Split(ctx context.Context, dbM *database.Meta, dbT *database.Oracle, cfg *config.Config, tables []database.Wait) error {
	sTime := time.Now()
	zap.L().Info("split mysql database decimal tables task starting", zap.String("startTime", sTime.String()))

	g := workpool.New(cfg.AppConfig.SQLThread)

	for _, tab := range tables {
		t := tab
		g.Do(func() error {
			mTime := time.Now()
			zap.L().Info("split mysql database decimal single table starting", zap.String("schema", strings.ToUpper(cfg.OracleConfig.Schema)), zap.String("table", strings.ToUpper(t.TableNameS)), zap.String("startTime", mTime.String()))

			taskName := uuid.New().String()

			if err := dbT.StartOracleChunkCreateTask(taskName); err != nil {
				return err
			}

			if err := dbT.StartOracleCreateChunkByRowID(taskName, strings.ToUpper(cfg.OracleConfig.Schema), strings.ToUpper(t.TableNameS), strconv.Itoa(cfg.AppConfig.ChunkSize)); err != nil {
				return err
			}

			chunkRes, err := dbT.GetOracleTableChunksByRowID(taskName)
			if err != nil {
				return err
			}

			if len(chunkRes) == 0 {
				var fs []database.Full

				fs = append(fs, database.Full{
					SchemaNameT:   strings.ToUpper(cfg.OracleConfig.Schema),
					TableNameT:    strings.ToUpper(t.TableNameS),
					SQLHint:       cfg.AppConfig.SQLHint,
					ColumnDetailT: strings.ToUpper(t.ColumnDetailS),
					ChunkDetailT:  `1 = 1`,
					TaskStatus:    "WAITING",
				})
				err = database.NewFullModel(dbM).BatchCreateFullSyncMeta(ctx, fs, cfg.AppConfig.BatchSize)
				if err != nil {
					return err
				}
				return nil
			}

			var fs []database.Full
			for _, res := range chunkRes {
				fs = append(fs, database.Full{
					SchemaNameT:   strings.ToUpper(cfg.OracleConfig.Schema),
					TableNameT:    strings.ToUpper(t.TableNameS),
					SQLHint:       cfg.AppConfig.SQLHint,
					ColumnDetailT: strings.ToUpper(t.ColumnDetailS),
					ChunkDetailT:  common.StringsBuilder(res["CMD"]),
					TaskStatus:    "WAITING",
				})
			}
			err = database.NewFullModel(dbM).BatchCreateFullSyncMeta(ctx, fs, cfg.AppConfig.BatchSize)
			if err != nil {
				return err
			}

			zap.L().Info("split mysql database decimal single table success", zap.String("schema", strings.ToUpper(cfg.OracleConfig.Schema)), zap.String("table", strings.ToUpper(t.TableNameS)), zap.String("cost", time.Now().Sub(mTime).String()))
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	zap.L().Info("split mysql database decimal tables task success", zap.String("cost", time.Now().Sub(sTime).String()))
	return nil
}

func Scan(ctx context.Context, dbM *database.Meta, dbT *database.Oracle, cfg *config.Config, tables []database.Wait) error {
	sTime := time.Now()
	zap.L().Info("scan oracle database schema tables task starting", zap.String("startTime", sTime.String()))

	bigintStr, err := decimal.NewFromString("9223372036854775807")
	if err != nil {
		return err
	}

	unsinBigintStr, err := decimal.NewFromString("18446744073709551615")
	if err != nil {
		return err
	}

	g0 := workpool.New(cfg.AppConfig.TableThread)

	for _, tab := range tables {
		t := tab
		g0.Do(func() error {
			mTime := time.Now()
			zap.L().Info("scan oracle database decimal single table starting", zap.String("schema", strings.ToUpper(cfg.OracleConfig.Schema)), zap.String("table", strings.ToUpper(t.TableNameS)), zap.String("starttime", mTime.String()))

			metas, err := database.NewFullModel(dbM).DetailFullSyncMeta(ctx, &database.Full{
				SchemaNameT: t.SchemaNameT,
				TableNameT:  t.TableNameS,
			})
			if err != nil {
				return err
			}

			g := workpool.New(cfg.AppConfig.SQLThread)

			for _, mt := range metas {
				m := mt
				g.Do(func() error {
					tTime := time.Now()
					zap.L().Info("scan oracle database decimal single table chunk starting", zap.String("schema", strings.ToUpper(cfg.OracleConfig.Schema)), zap.String("table", strings.ToUpper(t.TableNameS)), zap.String("column", m.ColumnDetailT), zap.String("chunk", m.ChunkDetailT), zap.String("startTime", tTime.String()))

					err = database.NewFullModel(dbM).UpdateFullSyncMetaChunk(ctx, &database.Full{
						SchemaNameT:  m.SchemaNameT,
						TableNameT:   m.TableNameT,
						ChunkDetailT: m.ChunkDetailT,
					}, map[string]interface{}{
						"TaskStatus": "RUNNING",
					})
					if err != nil {
						return err
					}

					scanResults, err := dbT.ScanOracleTableDecimalData(m, common.MigrateOracleCharsetStringConvertMapping[strings.ToUpper(cfg.OracleConfig.Charset)], common.MigrateMYSQLCompatibleCharsetStringConvertMapping[strings.ToUpper(cfg.MySQLConfig.Charset)], bigintStr, unsinBigintStr)
					if err != nil {
						return err
					}

					if len(scanResults) > 0 {
						err = database.NewScanModel(dbM).BatchCreateScanResult(ctx, scanResults, cfg.AppConfig.BatchSize)
						if err != nil {
							return err
						}
					}

					err = database.NewFullModel(dbM).UpdateFullSyncMetaChunk(ctx, &database.Full{
						SchemaNameT:  m.SchemaNameT,
						TableNameT:   m.TableNameT,
						ChunkDetailT: m.ChunkDetailT,
					}, map[string]interface{}{
						"TaskStatus": "SUCCESS",
					})
					if err != nil {
						return err
					}

					zap.L().Info("scan oracle database decimal single table chunk success", zap.String("schema", strings.ToUpper(cfg.OracleConfig.Schema)), zap.String("table", strings.ToUpper(t.TableNameS)), zap.String("column", m.ColumnDetailT), zap.String("chunk", m.ChunkDetailT), zap.String("cost", time.Now().Sub(tTime).String()))

					return nil
				})
			}

			if err = g.Wait(); err != nil {
				return err
			}

			zap.L().Info("scan oracle database decimal single tables success", zap.String("schema", strings.ToUpper(cfg.OracleConfig.Schema)), zap.String("table", strings.ToUpper(t.TableNameS)), zap.String("cost", time.Now().Sub(mTime).String()))
			return nil
		})
	}

	if err := g0.Wait(); err != nil {
		return err
	}

	zap.L().Info("scan oracle database schema tables task success", zap.String("schema", strings.ToUpper(cfg.OracleConfig.Schema)), zap.String("cost", time.Now().Sub(sTime).String()))

	return nil
}

func Statistics(ctx context.Context, dbM *database.Meta, dbS *database.MySQL, cfg *config.Config, tables []database.Wait) error {
	sTime := time.Now()
	zap.L().Info("statistics mysql database decimal tables task starting", zap.String("startTime", sTime.String()))

	g := workpool.New(cfg.AppConfig.SQLThread)

	for _, tab := range tables {
		t := tab
		g.Do(func() error {
			mTime := time.Now()
			zap.L().Info("statistics mysql database decimal single table starting", zap.String("schema", strings.ToUpper(cfg.OracleConfig.Schema)), zap.String("table", strings.ToUpper(t.TableNameS)), zap.String("startTime", mTime.String()))

			originColumns := strings.Split(t.ColumnDetailS, ",")

			columns, err := dbS.GetMySQLTableColumn(cfg.MySQLConfig.Schema, t.TableNameS)
			if err != nil {
				return err
			}

			results, err := database.NewScanModel(dbM).DetailScanResult(ctx, &database.Scan{
				SchemaNameT: strings.ToUpper(t.SchemaNameT),
				TableNameT:  strings.ToUpper(t.TableNameS),
			})
			if err != nil {
				return err
			}

			resl := make(map[string]struct{})
			for _, r := range results {
				resl[r.ColumnName] = struct{}{}
			}

			if len(results) == 0 {
				err = database.NewStatisticsModel(dbM).CreateStatistics(ctx, &database.Statistics{
					SchemaNameT:     strings.ToUpper(t.SchemaNameT),
					TableNameT:      strings.ToUpper(t.TableNameS),
					ModifyColumn:    "",
					NotModifyColumn: "",
				})
				if err != nil {
					return err
				}
			} else {
				var (
					canModify   []string
					canotModify []string
				)
				for _, c := range originColumns {
					if _, ok := resl[c]; ok {
						canotModify = append(canotModify, c)
					} else {
						for _, col := range columns {
							if strings.EqualFold(col["COLUMN_NAME"], c) {
								var sqlStr string

								switch {
								case strings.EqualFold(col["NULLABLE"], "N"):
									if strings.EqualFold(col["DATA_DEFAULT"], "NULLSTRING") {
										if strings.EqualFold(col["COMMENTS"], "") {
											sqlStr = fmt.Sprintf("ALTER TABLE `%s`.`%s` MODIFY `%s` BIGINT(20) NOT NULL", strings.ToUpper(cfg.MySQLConfig.Schema), t.TableNameS, c)
										} else {
											sqlStr = fmt.Sprintf("ALTER TABLE `%s`.`%s` MODIFY `%s` BIGINT(20) NOT NULL COMMENT '%s'", strings.ToUpper(cfg.MySQLConfig.Schema), t.TableNameS, c, col["COMMENTS"])
										}
									} else {
										if strings.EqualFold(col["COMMENTS"], "") {
											sqlStr = fmt.Sprintf("ALTER TABLE `%s`.`%s` MODIFY `%s` BIGINT(20) NOT NULL DEFAULT %s", strings.ToUpper(cfg.MySQLConfig.Schema), t.TableNameS, c, col["DATA_DEFAULT"])
										} else {
											sqlStr = fmt.Sprintf("ALTER TABLE `%s`.`%s` MODIFY `%s` BIGINT(20) NOT NULL DEFAULT %s COMMENT '%s'", strings.ToUpper(cfg.MySQLConfig.Schema), t.TableNameS, c, col["DATA_DEFAULT"], col["COMMENTS"])
										}
									}
								default:
									if strings.EqualFold(col["DATA_DEFAULT"], "NULLSTRING") {
										if strings.EqualFold(col["COMMENTS"], "") {
											sqlStr = fmt.Sprintf("ALTER TABLE `%s`.`%s` MODIFY `%s` BIGINT(20) DEFAULT NULL", strings.ToUpper(cfg.MySQLConfig.Schema), t.TableNameS, c)
										} else {
											sqlStr = fmt.Sprintf("ALTER TABLE `%s`.`%s` MODIFY `%s` BIGINT(20) DEFAULT NULL COMMENT '%s'", strings.ToUpper(cfg.MySQLConfig.Schema), t.TableNameS, c, col["COMMENTS"])
										}
									} else {
										if strings.EqualFold(col["COMMENTS"], "") {
											sqlStr = fmt.Sprintf("ALTER TABLE `%s`.`%s` MODIFY `%s` BIGINT(20) DEFAULT %s", strings.ToUpper(cfg.MySQLConfig.Schema), t.TableNameS, c, col["DATA_DEFAULT"])
										} else {
											sqlStr = fmt.Sprintf("ALTER TABLE `%s`.`%s` MODIFY `%s` BIGINT(20) DEFAULT %s COMMENT '%s'", strings.ToUpper(cfg.MySQLConfig.Schema), t.TableNameS, c, col["DATA_DEFAULT"], col["COMMENTS"])
										}
									}
								}

								canModify = append(canModify, sqlStr)
							}
						}
					}
				}
				err = database.NewStatisticsModel(dbM).CreateStatistics(ctx, &database.Statistics{
					SchemaNameT:     strings.ToUpper(t.SchemaNameT),
					TableNameT:      strings.ToUpper(t.TableNameS),
					ModifyColumn:    strings.Join(canModify, ";\n"),
					NotModifyColumn: strings.Join(canotModify, ","),
				})
				if err != nil {
					return err
				}
			}
			zap.L().Info("statistics mysql database decimal single table success", zap.String("schema", strings.ToUpper(cfg.OracleConfig.Schema)), zap.String("table", strings.ToUpper(t.TableNameS)), zap.String("cost", time.Now().Sub(mTime).String()))
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	zap.L().Info("statistics mysql database decimal tables task success", zap.String("cost", time.Now().Sub(sTime).String()))
	return nil
}
