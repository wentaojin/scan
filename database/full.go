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
	"strings"
)

type Full struct {
	ID            uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SchemaNameT   string `gorm:"type:varchar(100);not null;index:idx_dbtype_st_map,unique;comment:'目标端 schema'" json:"schema_name_t"`
	TableNameT    string `gorm:"type:varchar(100);not null;index:idx_dbtype_st_map,unique;comment:'目标端表名'" json:"table_name_t"`
	SQLHint       string `gorm:"type:varchar(300);comment:'sql hint'" json:"sql_hint"`
	ColumnDetailT string `gorm:"type:text;comment:'源端查询字段信息'" json:"column_detail_t"`
	ChunkDetailT  string `gorm:"type:varchar(300);not null;index:idx_dbtype_st_map,unique;comment:'表 chunk 切分信息'" json:"chunk_detail_t"`
	TaskStatus    string `gorm:"type:varchar(30);not null;comment:'任务 chunk 状态'" json:"task_status"`
	*Meta         `gorm:"-" json:"-"`
}

func NewFullModel(m *Meta) *Full {
	return &Full{
		Meta: m,
	}
}

func (rw *Full) ParseSchemaTable() (string, error) {
	stmt := &gorm.Statement{DB: rw.GormDB}
	err := stmt.Parse(rw)
	if err != nil {
		return "", fmt.Errorf("parse struct [Full] get table_name failed: %v", err)
	}
	return stmt.Schema.Table, nil
}

func (rw *Full) DetailFullSyncMeta(ctx context.Context, detailS *Full) ([]Full, error) {
	var dsMetas []Full
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return dsMetas, err
	}
	if err := rw.DB(ctx).Where(detailS).Find(&dsMetas).Error; err != nil {
		return dsMetas, fmt.Errorf("detail table [%s] record failed: %v", table, err)
	}
	return dsMetas, nil
}

func (rw *Full) BatchCreateFullSyncMeta(ctx context.Context, createS []Full, batchSize int) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	if err := rw.DB(ctx).CreateInBatches(createS, batchSize).Error; err != nil {
		return fmt.Errorf("batch create table [%s] record failed: %v", table, err)
	}
	return nil
}

func (rw *Full) UpdateFullSyncMetaChunk(ctx context.Context, detailS *Full, updates map[string]interface{}) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	if err = rw.DB(ctx).Model(Full{}).
		Where("schema_name_t = ? AND table_name_t = ? AND chunk_detail_t = ?",
			strings.ToUpper(detailS.SchemaNameT),
			strings.ToUpper(detailS.TableNameT),
			detailS.ChunkDetailT).
		Updates(updates).Error; err != nil {
		return fmt.Errorf("update table [%s] record failed: %v", table, err)
	}
	return nil
}
