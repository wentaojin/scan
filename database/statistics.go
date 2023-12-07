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

type Statistics struct {
	ID              uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SchemaNameT     string `gorm:"type:varchar(100);not null;index:idx_complex;comment:'目标端 schema'" json:"schema_name_t"`
	TableNameT      string `gorm:"type:varchar(100);not null;index:idx_complex;comment:'目标端表名'" json:"table_name_t"`
	ModifyColumn    string `gorm:"type:longtext;comment:'目标端表字段信息满足条件可 modify'" json:"modify_column"`
	NotModifyColumn string `gorm:"type:longtext;comment:'目标端表字段信息不满足条件不可 modify'" json:"not_modify_column"`
	*Meta           `gorm:"-" json:"-"`
}

func NewStatisticsModel(m *Meta) *Statistics {
	return &Statistics{
		Meta: m,
	}
}

func (rw *Statistics) ParseSchemaTable() (string, error) {
	stmt := &gorm.Statement{DB: rw.GormDB}
	err := stmt.Parse(rw)
	if err != nil {
		return "", fmt.Errorf("parse struct [Statistics] get table_name failed: %v", err)
	}
	return stmt.Schema.Table, nil
}

func (rw *Statistics) CreateStatistics(ctx context.Context, createS *Statistics) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	if err := rw.DB(ctx).Create(createS).Error; err != nil {
		return fmt.Errorf("create table [%s] record failed: %v", table, err)
	}
	return nil
}
