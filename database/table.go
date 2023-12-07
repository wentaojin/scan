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
	"fmt"
	"strings"
)

func (m *MySQL) GetMySQLTables(schemaName string) ([]string, error) {
	_, res, err := Query(m.Ctx, m.MySQLDB, fmt.Sprintf(`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES where UPPER(TABLE_SCHEMA) = '%s' AND TABLE_TYPE = 'BASE TABLE'`, strings.ToUpper(schemaName)))
	if err != nil {
		return []string{}, err
	}
	var tables []string
	if len(res) > 0 {
		for _, r := range res {
			tables = append(tables, r["TABLE_NAME"])
		}
	}

	return tables, nil
}

func (m *MySQL) GetMySQLTableColumn(schemaName, tableName string) ([]map[string]string, error) {
	var (
		res []map[string]string
		err error
	)

	_, res, err = Query(m.Ctx, m.MySQLDB, fmt.Sprintf(`SELECT COLUMN_NAME,
		DATA_TYPE,
		IFNULL(CHARACTER_MAXIMUM_LENGTH,0) DATA_LENGTH,
		IFNULL(NUMERIC_SCALE,0) DATA_SCALE,
		IFNULL(NUMERIC_PRECISION,0) DATA_PRECISION,
		IFNULL(DATETIME_PRECISION,0) DATETIME_PRECISION,
		IF(IS_NULLABLE = 'NO', 'N', 'Y') NULLABLE,
		IFNULL(COLUMN_DEFAULT,'NULLSTRING') DATA_DEFAULT,
		IFNULL(COLUMN_COMMENT,'') COMMENTS,
		IFNULL(CHARACTER_SET_NAME,'UNKNOWN') CHARACTER_SET_NAME,
		IFNULL(COLLATION_NAME,'UNKNOWN') COLLATION_NAME
 FROM information_schema.COLUMNS
 WHERE UPPER(TABLE_SCHEMA) = UPPER('%s')
   AND UPPER(TABLE_NAME) = UPPER('%s')
 ORDER BY ORDINAL_POSITION`, schemaName, tableName))

	if err != nil {
		return res, err
	}
	return res, nil
}
