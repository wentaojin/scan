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
package common

import (
	"bytes"
	"fmt"
	"github.com/scylladb/go-set"
	"github.com/scylladb/go-set/strset"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
	"golang.org/x/text/transform"
	"io"
	"strings"
	"time"
)

const (
	MySQLMaxIdleConn     = 512
	MySQLMaxConn         = 1024
	MySQLConnMaxLifeTime = 300 * time.Second
	MySQLConnMaxIdleTime = 200 * time.Second
)

const (
	CharsetUTF8MB4 = "UTF8MB4"
	CharsetGB18030 = "GB18030"
	CharsetBIG5    = "BIG5"
	CharsetGBK     = "GBK"
)

var (
	MYSQLCharsetUTF8MB4       = "UTF8MB4"
	MYSQLCharsetUTF8          = "UTF8"
	MYSQLCharsetBIG5          = "BIG5"
	MYSQLCharsetGBK           = "GBK"
	MYSQLCharsetGB18030       = "GB18030"
	ORACLECharsetAL32UTF8     = "AL32UTF8"
	ORACLECharsetZHT16BIG5    = "ZHT16BIG5"
	ORACLECharsetZHS16GBK     = "ZHS16GBK"
	ORACLECharsetZHS32GB18030 = "ZHS32GB18030"
)

var MigrateOracleCharsetStringConvertMapping = map[string]string{
	ORACLECharsetAL32UTF8:     CharsetUTF8MB4,
	ORACLECharsetZHT16BIG5:    CharsetBIG5,
	ORACLECharsetZHS16GBK:     CharsetGBK,
	ORACLECharsetZHS32GB18030: CharsetGB18030,
}

var MigrateMYSQLCompatibleCharsetStringConvertMapping = map[string]string{
	MYSQLCharsetUTF8MB4: CharsetUTF8MB4,
	MYSQLCharsetUTF8:    CharsetUTF8MB4,
	MYSQLCharsetBIG5:    CharsetBIG5,
	MYSQLCharsetGBK:     CharsetGBK,
	MYSQLCharsetGB18030: CharsetGB18030,
}

func StringsBuilder(str ...string) string {
	var b strings.Builder
	for _, p := range str {
		b.WriteString(p)
	}
	return b.String() // no copying
}

func CharsetConvert(data []byte, fromCharset, toCharset string) ([]byte, error) {
	switch {
	case strings.EqualFold(fromCharset, CharsetUTF8MB4) && strings.EqualFold(toCharset, CharsetGBK):
		reader := transform.NewReader(bytes.NewReader(data), encoding.ReplaceUnsupported(simplifiedchinese.GBK.NewEncoder()))
		gbkBytes, err := io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		return gbkBytes, nil

	case strings.EqualFold(fromCharset, CharsetUTF8MB4) && strings.EqualFold(toCharset, CharsetGB18030):
		reader := transform.NewReader(bytes.NewReader(data), encoding.ReplaceUnsupported(simplifiedchinese.GB18030.NewEncoder()))
		gbk18030Bytes, err := io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		return gbk18030Bytes, nil

	case strings.EqualFold(fromCharset, CharsetUTF8MB4) && strings.EqualFold(toCharset, CharsetBIG5):
		reader := transform.NewReader(bytes.NewReader(data), encoding.ReplaceUnsupported(traditionalchinese.Big5.NewEncoder()))
		bigBytes, err := io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		return bigBytes, nil

	case strings.EqualFold(fromCharset, CharsetUTF8MB4) && strings.EqualFold(toCharset, CharsetUTF8MB4):
		return data, nil

	case strings.EqualFold(fromCharset, CharsetGBK) && strings.EqualFold(toCharset, CharsetUTF8MB4):
		decoder := simplifiedchinese.GBK.NewDecoder()
		utf8Data, err := decoder.Bytes(data)
		if err != nil {
			return nil, err
		}

		return utf8Data, nil

	case strings.EqualFold(fromCharset, CharsetGB18030) && strings.EqualFold(toCharset, CharsetUTF8MB4):
		decoder := simplifiedchinese.GB18030.NewDecoder()
		utf8Data, err := decoder.Bytes(data)
		if err != nil {
			return nil, err
		}
		return utf8Data, nil

	case strings.EqualFold(fromCharset, CharsetBIG5) && strings.EqualFold(toCharset, CharsetUTF8MB4):
		decoder := traditionalchinese.Big5.NewDecoder()
		utf8Data, err := decoder.Bytes(data)
		if err != nil {
			return nil, err
		}

		return utf8Data, nil

	default:
		return nil, fmt.Errorf("from charset [%v], to charset [%v] convert isn't support", fromCharset, toCharset)
	}
}

func FilterIntersectionStringItems(originItems, newItems []string) []string {
	s1 := set.NewStringSet()
	for _, t := range originItems {
		s1.Add(strings.ToUpper(t))
	}
	s2 := set.NewStringSet()
	for _, t := range newItems {
		s2.Add(strings.ToUpper(t))
	}
	return strset.Intersection(s1, s2).List()
}
