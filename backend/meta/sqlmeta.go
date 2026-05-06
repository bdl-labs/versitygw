// Copyright 2026 Versity Software
// This file is licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package meta

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/versity/versitygw/s3err"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// SqlMeta is a MetadataStorer backed by SQLite using GORM.
type SqlMeta struct {
	db *gorm.DB
}

var _ MetadataStorer = SqlMeta{}

type metadataEntry struct {
	Bucket     string    `gorm:"column:bucket;primaryKey"`
	ObjectName string    `gorm:"column:object_name;primaryKey;index:idx_meta_bucket_object"`
	Attribute  string    `gorm:"column:attribute;primaryKey"`
	Value      []byte    `gorm:"column:value;not null"`
	CreatedAt  time.Time `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt  time.Time `gorm:"column:updated_at;autoUpdateTime"`
}

func (metadataEntry) TableName() string {
	return "metadata_entries"
}

func NewSqlMeta(dbPath string) (SqlMeta, error) {
	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	if err != nil {
		return SqlMeta{}, fmt.Errorf("open sqlite: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return SqlMeta{}, fmt.Errorf("get sql db: %w", err)
	}

	if _, err := sqlDB.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		return SqlMeta{}, fmt.Errorf("set WAL mode: %w", err)
	}
	if _, err := sqlDB.Exec("PRAGMA busy_timeout=5000;"); err != nil {
		return SqlMeta{}, fmt.Errorf("set busy_timeout: %w", err)
	}
	sqlDB.SetMaxOpenConns(4)
	sqlDB.SetMaxIdleConns(4)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)

	if err := db.AutoMigrate(&metadataEntry{}); err != nil {
		return SqlMeta{}, fmt.Errorf("migrate sqlite schema: %w", err)
	}

	return SqlMeta{db: db}, nil
}

func (s SqlMeta) RetrieveAttribute(_ *os.File, bucket, object, attribute string) ([]byte, error) {
	var entry metadataEntry
	err := s.db.
		Where("bucket = ? AND object_name = ? AND attribute = ?", bucket, object, attribute).
		First(&entry).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNoSuchKey
		}
		return nil, fmt.Errorf("retrieve attribute: %w", err)
	}
	return entry.Value, nil
}

func (s SqlMeta) StoreAttribute(_ *os.File, bucket, object, attribute string, value []byte) error {
	entry := metadataEntry{
		Bucket:     bucket,
		ObjectName: object,
		Attribute:  attribute,
		Value:      value,
	}
	if err := s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "bucket"}, {Name: "object_name"}, {Name: "attribute"}},
		DoUpdates: clause.AssignmentColumns([]string{"value", "updated_at"}),
	}).Create(&entry).Error; err != nil {
		return mapSQLError("store attribute", err)
	}
	return nil
}

func (s SqlMeta) DeleteAttribute(bucket, object, attribute string) error {
	res := s.db.Where("bucket = ? AND object_name = ? AND attribute = ?", bucket, object, attribute).Delete(&metadataEntry{})
	if res.Error != nil {
		return mapSQLError("delete attribute", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrNoSuchKey
	}
	return nil
}

func (s SqlMeta) ListAttributes(bucket, object string) ([]string, error) {
	var attrs []string
	if err := s.db.Model(&metadataEntry{}).
		Where("bucket = ? AND object_name = ?", bucket, object).
		Pluck("attribute", &attrs).Error; err != nil {
		return nil, mapSQLError("list attributes", err)
	}
	return attrs, nil
}

func (s SqlMeta) DeleteAttributes(bucket, object string) error {
	if object == "" {
		if err := s.db.Where("bucket = ?", bucket).Delete(&metadataEntry{}).Error; err != nil {
			return mapSQLError("delete bucket attributes", err)
		}
		return nil
	}

	if err := s.db.Where("bucket = ? AND object_name = ?", bucket, object).Delete(&metadataEntry{}).Error; err != nil {
		return mapSQLError("delete attributes", err)
	}
	return nil
}

func (s SqlMeta) RenameObject(bucket, oldObject, newObject string) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&metadataEntry{}).
			Where("bucket = ? AND object_name = ?", bucket, oldObject).
			Update("object_name", newObject).Error; err != nil {
			return mapSQLError("rename object metadata", err)
		}

		return nil
	})
}

func mapSQLError(op string, err error) error {
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "database or disk is full"), strings.Contains(msg, "no space left"):
		return s3err.GetAPIError(s3err.ErrNoSpaceLeftOnDevice)
	case strings.Contains(msg, "readonly"), strings.Contains(msg, "read-only"):
		return s3err.GetAPIError(s3err.ErrMethodNotAllowed)
	default:
		return fmt.Errorf("%s: %w", op, err)
	}
}
