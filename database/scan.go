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
	"fmt"
	"gorm.io/gorm"
)

type Scan struct {
	ID            uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SchemaNameT   string `gorm:"type:varchar(100);not null;index:idx_complex;comment:'目标端 schema'" json:"schema_name_t"`
	TableNameT    string `gorm:"type:varchar(100);not null;index:idx_complex;comment:'目标端表名'" json:"table_name_t"`
	SQLHint       string `gorm:"type:varchar(300);comment:'sql hint'" json:"sql_hint"`
	ColumnDetailT string `gorm:"type:longtext;comment:'源端查询字段信息'" json:"column_detail_t"`
	ChunkDetailT  string `gorm:"type:varchar(300);not null;comment:'表 chunk 切分信息'" json:"chunk_detail_t"`
	RowID         string `gorm:"type:varchar(300);not null;index:idx_complex;comment:'表异常数据所在行 rowid'" json:"row_id"`
	*Column
	*Meta `gorm:"-" json:"-"`
}

type Column struct {
	ColumnName           string `gorm:"type:varchar(300);not null;comment:'表异常数据所在行 rowid 字段名'" json:"column_name"`
	ColumnValue          string `gorm:"type:varchar(300);not null;comment:'表异常数据所在行 rowid 字段值'" json:"column_value"`
	ColumnBigint         string `gorm:"type:varchar(300);not null;comment:'表异常数据所在行 rowid 字段是否超过 bigint, eg: UNKNOWN、LESS、MORE'" json:"column_bigint"`
	ColumnUnsingedBigint string `gorm:"type:varchar(300);not null;comment:'表异常数据所在行 rowid 字段是否超过 unsinged bigint, eg: UNKNOWN、LESS、MORE'" json:"column_unsinged_bigint"`
}

func NewScanModel(m *Meta) *Scan {
	return &Scan{
		Meta: m,
	}
}

func (rw *Scan) ParseSchemaTable() (string, error) {
	stmt := &gorm.Statement{DB: rw.GormDB}
	err := stmt.Parse(rw)
	if err != nil {
		return "", fmt.Errorf("parse struct [Scan] get table_name failed: %v", err)
	}
	return stmt.Schema.Table, nil
}

func (rw *Scan) DetailScanResult(ctx context.Context, detailS *Scan) ([]Scan, error) {
	var dsMetas []Scan
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return dsMetas, err
	}
	if err := rw.DB(ctx).Where(detailS).Find(&dsMetas).Error; err != nil {
		return dsMetas, fmt.Errorf("detail table [%s] record failed: %v", table, err)
	}
	return dsMetas, nil
}

func (rw *Scan) BatchCreateScanResult(ctx context.Context, createS []Scan, batchSize int) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	if err := rw.DB(ctx).CreateInBatches(createS, batchSize).Error; err != nil {
		return fmt.Errorf("batch create table [%s] record failed: %v", table, err)
	}
	return nil
}
