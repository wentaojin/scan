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

type Wait struct {
	ID            uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SchemaNameT   string `gorm:"type:varchar(100);not null;index:idx_dbtype_st_map,unique;comment:'目标端 schema'" json:"schema_name_t"`
	TableNameS    string `gorm:"type:varchar(100);not null;index:idx_dbtype_st_map,unique;comment:'源端表名'" json:"table_name_s"`
	ColumnDetailS string `gorm:"type:varchar(100);not null;" json:"column_detail_s"`
	*Meta         `gorm:"-" json:"-"`
}

func NewWaitModel(m *Meta) *Wait {
	return &Wait{Meta: m}
}

func (rw *Wait) ParseSchemaTable() (string, error) {
	stmt := &gorm.Statement{DB: rw.GormDB}
	err := stmt.Parse(rw)
	if err != nil {
		return "", fmt.Errorf("parse struct [Wait] get table_name failed: %v", err)
	}
	return stmt.Schema.Table, nil
}

func (rw *Wait) CreateWaitSyncMeta(ctx context.Context, createS *Wait) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	if err = rw.DB(ctx).Create(createS).Error; err != nil {
		return fmt.Errorf("create table [%s] record failed: %v", table, err)
	}
	return nil
}

func (rw *Wait) DetailWaitSyncMeta(ctx context.Context, detailS *Wait) ([]Wait, error) {
	var dsMetas []Wait
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return dsMetas, err
	}
	if err = rw.DB(ctx).Where(detailS).Find(&dsMetas).Error; err != nil {
		return dsMetas, fmt.Errorf("detail table [%s] record failed: %v", table, err)
	}
	return dsMetas, nil
}
