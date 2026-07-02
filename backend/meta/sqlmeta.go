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
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/versity/versitygw/s3err"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
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

// BurnDiscExtent is one on-media region for a logical object segment (a segment may have several).
type BurnDiscExtent struct {
	DiscAddress string `json:"discAddress"`
	FileSize    int64  `json:"fileSize"`
}

// BurnSegmentState is persisted per logical segment for BurnBridge PutObject (resume / visibility of burn progress).
type BurnSegmentState int

const (
	// BurnSegmentPending: segment issued or in flight; recorder ack not committed yet.
	BurnSegmentPending BurnSegmentState = 0
	// BurnSegmentSucceeded: recorder acknowledged successful placement on media for this segment.
	BurnSegmentSucceeded BurnSegmentState = 1
	// BurnSegmentFailed: recorder reported failure or gateway could not validate the segment ack.
	BurnSegmentFailed BurnSegmentState = 2
)

// burnbridgeObjectSegment stores per-chunk state for BurnBridge PutObject resume (checksum, offset, burn ack).
type burnbridgeObjectSegment struct {
	Bucket       string `gorm:"column:bucket;primaryKey"`
	ObjectName   string `gorm:"column:object_name;primaryKey;index:idx_bb_seg_bucket_object"`
	SegmentIndex int    `gorm:"column:segment_index;primaryKey"`
	MediaID      string `gorm:"column:media_id;size:128;not null;default:''"`
	ByteOffset   int64  `gorm:"column:byte_offset;not null"`
	ByteSize     int64  `gorm:"column:byte_size;not null"`
	ChecksumMD5  string `gorm:"column:checksum_md5;size:32;not null"`
	BurnState    int    `gorm:"column:burn_state;not null;default:0"`
	// DiscExtents is JSON: []BurnDiscExtent
	DiscExtents string `gorm:"column:disc_extents;type:text"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

func (burnbridgeObjectSegment) TableName() string {
	return "burnbridge_object_segments"
}

const burnbridgeRestoreBatchSize = 50

// BurnObjectSegment is returned by GetBurnObjectSegment for BurnBridge chunk lookups.
type BurnObjectSegment struct {
	MediaID     string
	ByteOffset  int64
	ByteSize    int64
	ChecksumMD5 string
	State       BurnSegmentState
	DiscExtents []BurnDiscExtent
}

// BurnObjectSegmentDetail includes segment index for ordered manifest assembly.
type BurnObjectSegmentDetail struct {
	SegmentIndex int
	BurnObjectSegment
}

// BurnUploadKind distinguishes implicit single-stream uploads from standard multipart sessions.
type BurnUploadKind string

const (
	BurnUploadKindSingle    BurnUploadKind = "single"
	BurnUploadKindMultipart BurnUploadKind = "multipart"
)

// BurnUploadState tracks the current gateway-side state of an upload session or part.
type BurnUploadState string

const (
	BurnUploadStateWriting   BurnUploadState = "writing"
	BurnUploadStateCompleted BurnUploadState = "completed"
	BurnUploadStateAborted   BurnUploadState = "aborted"
	BurnUploadStateFailed    BurnUploadState = "failed"
)

type burnbridgeUploadSession struct {
	Bucket         string    `gorm:"column:bucket;primaryKey"`
	ObjectName     string    `gorm:"column:object_name;primaryKey;index:idx_bb_upload_session_object"`
	UploadID       string    `gorm:"column:upload_id;primaryKey;size:128"`
	Kind           string    `gorm:"column:kind;size:32;not null"`
	State          string    `gorm:"column:state;size:32;not null"`
	MediaID        string    `gorm:"column:media_id;size:128;not null;default:''"`
	RecorderJobID  string    `gorm:"column:recorder_job_id;size:128;not null;default:''"`
	ContentLength  int64     `gorm:"column:content_length;not null;default:0"`
	BytesReceived  int64     `gorm:"column:bytes_received;not null;default:0"`
	NextPartNumber int       `gorm:"column:next_part_number;not null;default:1"`
	CreatedAt      time.Time `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt      time.Time `gorm:"column:updated_at;autoUpdateTime"`
}

func (burnbridgeUploadSession) TableName() string {
	return "burnbridge_upload_sessions"
}

// BurnUploadSessionRecord exposes persisted upload session state.
type BurnUploadSessionRecord struct {
	Bucket         string
	ObjectName     string
	UploadID       string
	Kind           BurnUploadKind
	State          BurnUploadState
	MediaID        string
	RecorderJobID  string
	ContentLength  int64
	BytesReceived  int64
	NextPartNumber int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type burnbridgeUploadPart struct {
	Bucket        string    `gorm:"column:bucket;primaryKey"`
	ObjectName    string    `gorm:"column:object_name;primaryKey;index:idx_bb_upload_part_object"`
	UploadID      string    `gorm:"column:upload_id;primaryKey;size:128"`
	PartNumber    int       `gorm:"column:part_number;primaryKey"`
	StartOffset   int64     `gorm:"column:start_offset;not null;default:0"`
	BytesReceived int64     `gorm:"column:bytes_received;not null;default:0"`
	PartSize      int64     `gorm:"column:part_size;not null;default:0"`
	ChecksumMD5   string    `gorm:"column:checksum_md5;size:32;not null;default:''"`
	ETag          string    `gorm:"column:etag;size:64;not null;default:''"`
	State         string    `gorm:"column:state;size:32;not null"`
	SegmentCount  int       `gorm:"column:segment_count;not null;default:0"`
	CreatedAt     time.Time `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt     time.Time `gorm:"column:updated_at;autoUpdateTime"`
}

func (burnbridgeUploadPart) TableName() string {
	return "burnbridge_upload_parts"
}

// BurnUploadPartRecord exposes persisted part-level state for multipart / resumable uploads.
type BurnUploadPartRecord struct {
	Bucket        string
	ObjectName    string
	UploadID      string
	PartNumber    int
	StartOffset   int64
	BytesReceived int64
	PartSize      int64
	ChecksumMD5   string
	ETag          string
	State         BurnUploadState
	SegmentCount  int
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type BurnbridgeBucketBackup struct {
	Bucket        string                  `json:"bucket"`
	BackedUpAtUtc string                  `json:"backedUpAtUtc"`
	MetadataRows  []burnbridgeMetadataRow `json:"metadataRows"`
	SegmentRows   []burnbridgeSegmentRow  `json:"segmentRows"`
	SessionRows   []burnbridgeSessionRow  `json:"sessionRows"`
	PartRows      []burnbridgePartRow     `json:"partRows"`
}

type burnbridgeMetadataRow struct {
	ObjectName string `json:"objectName"`
	Attribute  string `json:"attribute"`
	Value      []byte `json:"value"`
}

type burnbridgeSegmentRow struct {
	ObjectName   string    `json:"objectName"`
	SegmentIndex int       `json:"segmentIndex"`
	MediaID      string    `json:"mediaId"`
	ByteOffset   int64     `json:"byteOffset"`
	ByteSize     int64     `json:"byteSize"`
	ChecksumMD5  string    `json:"checksumMd5"`
	BurnState    int       `json:"burnState"`
	DiscExtents  string    `json:"discExtents"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

type burnbridgeSessionRow struct {
	ObjectName     string    `json:"objectName"`
	UploadID       string    `json:"uploadId"`
	Kind           string    `json:"kind"`
	State          string    `json:"state"`
	MediaID        string    `json:"mediaId"`
	RecorderJobID  string    `json:"recorderJobId"`
	ContentLength  int64     `json:"contentLength"`
	BytesReceived  int64     `json:"bytesReceived"`
	NextPartNumber int       `json:"nextPartNumber"`
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
}

type burnbridgePartRow struct {
	ObjectName    string    `json:"objectName"`
	UploadID      string    `json:"uploadId"`
	PartNumber    int       `json:"partNumber"`
	StartOffset   int64     `json:"startOffset"`
	BytesReceived int64     `json:"bytesReceived"`
	PartSize      int64     `json:"partSize"`
	ChecksumMD5   string    `json:"checksumMd5"`
	ETag          string    `json:"etag"`
	State         string    `json:"state"`
	SegmentCount  int       `json:"segmentCount"`
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
}

// Burned reports whether this segment is known to have been placed successfully on media.
func (s BurnObjectSegment) Burned() bool { return s.State == BurnSegmentSucceeded }

func (s SqlMeta) withDB(op string, fn func(*gorm.DB) error) error {
	return sqliteWithRetry(op, func() error { return fn(s.db) })
}

func NewSqlMeta(dbPath string, opts ...SqlMetaOption) (SqlMeta, error) {
	var init sqlMetaInit
	for _, o := range opts {
		o(&init)
	}

	dsn, err := buildSQLiteDSN(dbPath)
	if err != nil {
		return SqlMeta{}, err
	}

	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		SkipDefaultTransaction: true,
		Logger: logger.New(
			log.New(os.Stdout, "", log.LstdFlags),
			logger.Config{
				IgnoreRecordNotFoundError: true,
				LogLevel:                  logger.Warn,
			}),
	})
	if err != nil {
		return SqlMeta{}, fmt.Errorf("open sqlite: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return SqlMeta{}, fmt.Errorf("get sql db: %w", err)
	}

	log := init.maintLog
	if log == nil {
		log = slog.Default()
	}
	if result, err := verifySQLiteIntegrity(sqlDB); err != nil {
		backupPath, backupErr := copySQLiteFileForDiagnostics(dbPath, "integrity-failed", log)
		if backupErr != nil {
			log.Warn("sqlite diagnostic backup failed after integrity check failure", "db_path", dbPath, "err", backupErr)
		}
		return SqlMeta{}, fmt.Errorf("sqlite integrity check failed: %w (result=%s, diagnostic_backup=%s)", err, result, backupPath)
	}
	if _, err := backupSQLiteDatabase(sqlDB, dbPath, "startup", log); err != nil {
		log.Warn("sqlite startup backup failed", "db_path", dbPath, "err", err)
	}
	applySQLiteFlashPragmas(sqlDB, log)

	sqlDB.SetMaxOpenConns(4)
	sqlDB.SetMaxIdleConns(4)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)

	if err := sqliteWithRetry("auto migrate", func() error {
		return db.AutoMigrate(
			&metadataEntry{},
			&burnbridgeObjectSegment{},
			&burnbridgeUploadSession{},
			&burnbridgeUploadPart{},
		)
	}); err != nil {
		return SqlMeta{}, fmt.Errorf("migrate sqlite schema: %w", err)
	}
	if db.Migrator().HasColumn(&burnbridgeObjectSegment{}, "burned") {
		_ = sqliteWithRetry("migrate burned column", func() error {
			return db.Exec("UPDATE burnbridge_object_segments SET burn_state = ? WHERE burned = ?", BurnSegmentSucceeded, true).Error
		})
	}

	if init.maintCtx != nil {
		go startFlashMaintenance(init.maintCtx, sqlDB, log)
	}

	return SqlMeta{db: db}, nil
}

func (s SqlMeta) Close() error {
	if s.db == nil {
		return nil
	}
	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// IsOpen reports whether this SqlMeta has an initialized database handle.
func (s SqlMeta) IsOpen() bool {
	return s.db != nil
}

func parseDiscExtentsJSON(s string) ([]BurnDiscExtent, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	var out []BurnDiscExtent
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, fmt.Errorf("parse disc_extents: %w", err)
	}
	return out, nil
}

func marshalDiscExtentsJSON(extents []BurnDiscExtent) (string, error) {
	if len(extents) == 0 {
		return "[]", nil
	}
	b, err := json.Marshal(extents)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// GetBurnObjectSegment returns one segment row or ErrNoSuchKey.
func (s SqlMeta) GetBurnObjectSegment(bucket, object string, segmentIndex int) (BurnObjectSegment, error) {
	var out BurnObjectSegment
	err := s.withDB("get burn object segment", func(db *gorm.DB) error {
		var row burnbridgeObjectSegment
		tx := db.Where("bucket = ? AND object_name = ? AND segment_index = ?", bucket, object, segmentIndex).Limit(1).Find(&row)
		if tx.Error != nil {
			return mapSQLError("get burn object segment", tx.Error)
		}
		if tx.RowsAffected == 0 {
			return ErrNoSuchKey
		}
		extents, perr := parseDiscExtentsJSON(row.DiscExtents)
		if perr != nil {
			return perr
		}
		out = BurnObjectSegment{
			MediaID:     row.MediaID,
			ByteOffset:  row.ByteOffset,
			ByteSize:    row.ByteSize,
			ChecksumMD5: row.ChecksumMD5,
			State:       BurnSegmentState(row.BurnState),
			DiscExtents: extents,
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrNoSuchKey) {
			return BurnObjectSegment{}, ErrNoSuchKey
		}
		return BurnObjectSegment{}, err
	}
	return out, nil
}

// UpsertBurnObjectSegment inserts or replaces per-segment metadata (pending, succeeded, or failed).
// extents may be nil or empty if the recorder has not reported on-media placement yet; stored as JSON [].
func (s SqlMeta) UpsertBurnObjectSegment(bucket, object, mediaID string, segmentIndex int, byteOffset, byteSize int64, checksumMD5 string, state BurnSegmentState, extents []BurnDiscExtent) error {
	discJSON, err := marshalDiscExtentsJSON(extents)
	if err != nil {
		return fmt.Errorf("burn segment disc_extents: %w", err)
	}
	row := burnbridgeObjectSegment{
		Bucket:       bucket,
		ObjectName:   object,
		SegmentIndex: segmentIndex,
		MediaID:      strings.TrimSpace(mediaID),
		ByteOffset:   byteOffset,
		ByteSize:     byteSize,
		ChecksumMD5:  checksumMD5,
		BurnState:    int(state),
		DiscExtents:  discJSON,
	}
	return s.withDB("upsert burn object segment", func(db *gorm.DB) error {
		if err := db.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "bucket"}, {Name: "object_name"}, {Name: "segment_index"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"byte_offset", "byte_size", "checksum_md5", "burn_state", "disc_extents", "updated_at",
			}),
		}).Create(&row).Error; err != nil {
			return mapSQLError("upsert burn object segment", err)
		}
		return nil
	})
}

// DeleteBurnObjectSegments removes all chunk rows for an object (e.g. content changed at segment 0).
func (s SqlMeta) DeleteBurnObjectSegments(bucket, object string) error {
	return s.withDB("delete burn object segments", func(db *gorm.DB) error {
		query := db.Where("bucket = ?", bucket)
		if strings.TrimSpace(object) != "" {
			query = query.Where("object_name = ?", object)
		}
		if err := query.Delete(&burnbridgeObjectSegment{}).Error; err != nil {
			return mapSQLError("delete burn object segments", err)
		}
		return nil
	})
}

// DeleteBurnObjectSegmentsFrom removes tail segment rows for an object starting at fromSegmentIndex (inclusive).
func (s SqlMeta) DeleteBurnObjectSegmentsFrom(bucket, object string, fromSegmentIndex int) error {
	_, err := s.DeleteBurnObjectSegmentsFromWithCount(bucket, object, fromSegmentIndex)
	return err
}

// DeleteBurnObjectSegmentsFromWithCount removes tail segment rows and returns deleted row count.
func (s SqlMeta) DeleteBurnObjectSegmentsFromWithCount(bucket, object string, fromSegmentIndex int) (int64, error) {
	var deleted int64
	if err := s.withDB("delete burn object segments from index", func(db *gorm.DB) error {
		res := db.Where("bucket = ? AND object_name = ? AND segment_index >= ?", bucket, object, fromSegmentIndex).
			Delete(&burnbridgeObjectSegment{})
		if err := res.Error; err != nil {
			return mapSQLError("delete burn object segments from index", err)
		}
		deleted = res.RowsAffected
		return nil
	}); err != nil {
		return 0, err
	}
	return deleted, nil
}

// ListBurnObjectSegments returns all segment rows for an object ordered by segment_index.
func (s SqlMeta) ListBurnObjectSegments(bucket, object string) ([]BurnObjectSegmentDetail, error) {
	var out []BurnObjectSegmentDetail
	err := s.withDB("list burn object segments", func(db *gorm.DB) error {
		var rows []burnbridgeObjectSegment
		if err := db.Where("bucket = ? AND object_name = ?", bucket, object).
			Order("segment_index ASC").
			Find(&rows).Error; err != nil {
			return mapSQLError("list burn object segments", err)
		}

		result := make([]BurnObjectSegmentDetail, 0, len(rows))
		for _, row := range rows {
			extents, perr := parseDiscExtentsJSON(row.DiscExtents)
			if perr != nil {
				return perr
			}
			result = append(result, BurnObjectSegmentDetail{
				SegmentIndex: row.SegmentIndex,
				BurnObjectSegment: BurnObjectSegment{
					MediaID:     row.MediaID,
					ByteOffset:  row.ByteOffset,
					ByteSize:    row.ByteSize,
					ChecksumMD5: row.ChecksumMD5,
					State:       BurnSegmentState(row.BurnState),
					DiscExtents: extents,
				},
			})
		}
		out = result
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// UpsertBurnUploadSession inserts or updates one gateway upload session row.
func (s SqlMeta) UpsertBurnUploadSession(rec BurnUploadSessionRecord) error {
	row := burnbridgeUploadSession{
		Bucket:         rec.Bucket,
		ObjectName:     rec.ObjectName,
		UploadID:       rec.UploadID,
		Kind:           string(rec.Kind),
		State:          string(rec.State),
		MediaID:        strings.TrimSpace(rec.MediaID),
		RecorderJobID:  strings.TrimSpace(rec.RecorderJobID),
		ContentLength:  rec.ContentLength,
		BytesReceived:  rec.BytesReceived,
		NextPartNumber: rec.NextPartNumber,
	}
	return s.withDB("upsert burn upload session", func(db *gorm.DB) error {
		if err := db.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "bucket"}, {Name: "object_name"}, {Name: "upload_id"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"kind", "state", "media_id", "recorder_job_id", "content_length", "bytes_received", "next_part_number", "updated_at",
			}),
		}).Create(&row).Error; err != nil {
			return mapSQLError("upsert burn upload session", err)
		}
		return nil
	})
}

// GetBurnUploadSession loads one persisted upload session or ErrNoSuchKey.
func (s SqlMeta) GetBurnUploadSession(bucket, object, uploadID string) (*BurnUploadSessionRecord, error) {
	var out *BurnUploadSessionRecord
	err := s.withDB("get burn upload session", func(db *gorm.DB) error {
		var row burnbridgeUploadSession
		tx := db.Where("bucket = ? AND object_name = ? AND upload_id = ?", bucket, object, uploadID).Limit(1).Find(&row)
		if tx.Error != nil {
			return mapSQLError("get burn upload session", tx.Error)
		}
		if tx.RowsAffected == 0 {
			return ErrNoSuchKey
		}
		out = &BurnUploadSessionRecord{
			Bucket:         row.Bucket,
			ObjectName:     row.ObjectName,
			UploadID:       row.UploadID,
			Kind:           BurnUploadKind(row.Kind),
			State:          BurnUploadState(row.State),
			MediaID:        row.MediaID,
			RecorderJobID:  row.RecorderJobID,
			ContentLength:  row.ContentLength,
			BytesReceived:  row.BytesReceived,
			NextPartNumber: row.NextPartNumber,
			CreatedAt:      row.CreatedAt,
			UpdatedAt:      row.UpdatedAt,
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrNoSuchKey) {
			return nil, ErrNoSuchKey
		}
		return nil, err
	}
	return out, nil
}

// DeleteBurnUploadSession removes one upload session row.
func (s SqlMeta) DeleteBurnUploadSession(bucket, object, uploadID string) error {
	return s.withDB("delete burn upload session", func(db *gorm.DB) error {
		query := db.Where("bucket = ? AND object_name = ?", bucket, object)
		if strings.TrimSpace(uploadID) != "" {
			query = query.Where("upload_id = ?", uploadID)
		}
		if err := query.Delete(&burnbridgeUploadSession{}).Error; err != nil {
			return mapSQLError("delete burn upload session", err)
		}
		return nil
	})
}

// ListBurnUploadSessions returns all upload sessions for an object ordered by upload id.
func (s SqlMeta) ListBurnUploadSessions(bucket, object string) ([]BurnUploadSessionRecord, error) {
	var out []BurnUploadSessionRecord
	err := s.withDB("list burn upload sessions", func(db *gorm.DB) error {
		var rows []burnbridgeUploadSession
		if err := db.Where("bucket = ? AND object_name = ?", bucket, object).
			Order("upload_id ASC").
			Find(&rows).Error; err != nil {
			return mapSQLError("list burn upload sessions", err)
		}
		result := make([]BurnUploadSessionRecord, 0, len(rows))
		for _, row := range rows {
			result = append(result, BurnUploadSessionRecord{
				Bucket:         row.Bucket,
				ObjectName:     row.ObjectName,
				UploadID:       row.UploadID,
				Kind:           BurnUploadKind(row.Kind),
				State:          BurnUploadState(row.State),
				MediaID:        row.MediaID,
				RecorderJobID:  row.RecorderJobID,
				ContentLength:  row.ContentLength,
				BytesReceived:  row.BytesReceived,
				NextPartNumber: row.NextPartNumber,
				CreatedAt:      row.CreatedAt,
				UpdatedAt:      row.UpdatedAt,
			})
		}
		out = result
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListBurnUploadSessionsByBucket returns all upload sessions in a bucket ordered by object/upload id.
func (s SqlMeta) ListBurnUploadSessionsByBucket(bucket string) ([]BurnUploadSessionRecord, error) {
	var out []BurnUploadSessionRecord
	err := s.withDB("list burn upload sessions by bucket", func(db *gorm.DB) error {
		var rows []burnbridgeUploadSession
		if err := db.Where("bucket = ?", bucket).
			Order("object_name ASC").
			Order("upload_id ASC").
			Find(&rows).Error; err != nil {
			return mapSQLError("list burn upload sessions by bucket", err)
		}
		result := make([]BurnUploadSessionRecord, 0, len(rows))
		for _, row := range rows {
			result = append(result, BurnUploadSessionRecord{
				Bucket:         row.Bucket,
				ObjectName:     row.ObjectName,
				UploadID:       row.UploadID,
				Kind:           BurnUploadKind(row.Kind),
				State:          BurnUploadState(row.State),
				MediaID:        row.MediaID,
				RecorderJobID:  row.RecorderJobID,
				ContentLength:  row.ContentLength,
				BytesReceived:  row.BytesReceived,
				NextPartNumber: row.NextPartNumber,
				CreatedAt:      row.CreatedAt,
				UpdatedAt:      row.UpdatedAt,
			})
		}
		out = result
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// UpsertBurnUploadPart inserts or updates one upload part row.
func (s SqlMeta) UpsertBurnUploadPart(rec BurnUploadPartRecord) error {
	row := burnbridgeUploadPart{
		Bucket:        rec.Bucket,
		ObjectName:    rec.ObjectName,
		UploadID:      rec.UploadID,
		PartNumber:    rec.PartNumber,
		StartOffset:   rec.StartOffset,
		BytesReceived: rec.BytesReceived,
		PartSize:      rec.PartSize,
		ChecksumMD5:   rec.ChecksumMD5,
		ETag:          rec.ETag,
		State:         string(rec.State),
		SegmentCount:  rec.SegmentCount,
	}
	return s.withDB("upsert burn upload part", func(db *gorm.DB) error {
		if err := db.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "bucket"}, {Name: "object_name"}, {Name: "upload_id"}, {Name: "part_number"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"start_offset", "bytes_received", "part_size", "checksum_md5", "etag", "state", "segment_count", "updated_at",
			}),
		}).Create(&row).Error; err != nil {
			return mapSQLError("upsert burn upload part", err)
		}
		return nil
	})
}

// DeleteBurnUploadParts removes all parts for one upload session, or all object parts when uploadID is blank.
func (s SqlMeta) DeleteBurnUploadParts(bucket, object, uploadID string) error {
	return s.withDB("delete burn upload parts", func(db *gorm.DB) error {
		query := db.Where("bucket = ? AND object_name = ?", bucket, object)
		if strings.TrimSpace(uploadID) != "" {
			query = query.Where("upload_id = ?", uploadID)
		}
		if err := query.Delete(&burnbridgeUploadPart{}).Error; err != nil {
			return mapSQLError("delete burn upload parts", err)
		}
		return nil
	})
}

// ListBurnUploadParts returns all parts for one upload session ordered by part number.
func (s SqlMeta) ListBurnUploadParts(bucket, object, uploadID string) ([]BurnUploadPartRecord, error) {
	var out []BurnUploadPartRecord
	err := s.withDB("list burn upload parts", func(db *gorm.DB) error {
		var rows []burnbridgeUploadPart
		if err := db.Where("bucket = ? AND object_name = ? AND upload_id = ?", bucket, object, uploadID).
			Order("part_number ASC").
			Find(&rows).Error; err != nil {
			return mapSQLError("list burn upload parts", err)
		}
		result := make([]BurnUploadPartRecord, 0, len(rows))
		for _, row := range rows {
			result = append(result, BurnUploadPartRecord{
				Bucket:        row.Bucket,
				ObjectName:    row.ObjectName,
				UploadID:      row.UploadID,
				PartNumber:    row.PartNumber,
				StartOffset:   row.StartOffset,
				BytesReceived: row.BytesReceived,
				PartSize:      row.PartSize,
				ChecksumMD5:   row.ChecksumMD5,
				ETag:          row.ETag,
				State:         BurnUploadState(row.State),
				SegmentCount:  row.SegmentCount,
				CreatedAt:     row.CreatedAt,
				UpdatedAt:     row.UpdatedAt,
			})
		}
		out = result
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetBurnUploadPart loads one persisted upload part row or ErrNoSuchKey.
func (s SqlMeta) GetBurnUploadPart(bucket, object, uploadID string, partNumber int) (*BurnUploadPartRecord, error) {
	var out *BurnUploadPartRecord
	err := s.withDB("get burn upload part", func(db *gorm.DB) error {
		var row burnbridgeUploadPart
		tx := db.Where("bucket = ? AND object_name = ? AND upload_id = ? AND part_number = ?",
			bucket, object, uploadID, partNumber).
			Limit(1).
			Find(&row)
		if tx.Error != nil {
			return mapSQLError("get burn upload part", tx.Error)
		}
		if tx.RowsAffected == 0 {
			return ErrNoSuchKey
		}
		out = &BurnUploadPartRecord{
			Bucket:        row.Bucket,
			ObjectName:    row.ObjectName,
			UploadID:      row.UploadID,
			PartNumber:    row.PartNumber,
			StartOffset:   row.StartOffset,
			BytesReceived: row.BytesReceived,
			PartSize:      row.PartSize,
			ChecksumMD5:   row.ChecksumMD5,
			ETag:          row.ETag,
			State:         BurnUploadState(row.State),
			SegmentCount:  row.SegmentCount,
			CreatedAt:     row.CreatedAt,
			UpdatedAt:     row.UpdatedAt,
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrNoSuchKey) {
			return nil, ErrNoSuchKey
		}
		return nil, err
	}
	return out, nil
}

func (s SqlMeta) ExportBurnbridgeBucket(bucket string) (*BurnbridgeBucketBackup, error) {
	trimmedBucket := strings.TrimSpace(bucket)
	if trimmedBucket == "" {
		return nil, fmt.Errorf("export burnbridge bucket: empty bucket")
	}

	backup := &BurnbridgeBucketBackup{
		Bucket:        trimmedBucket,
		BackedUpAtUtc: time.Now().UTC().Format(time.RFC3339Nano),
	}

	err := s.withDB("export burnbridge bucket", func(db *gorm.DB) error {
		var metadataRows []metadataEntry
		if err := db.Where("bucket = ?", trimmedBucket).Order("object_name ASC, attribute ASC").Find(&metadataRows).Error; err != nil {
			return mapSQLError("export burnbridge bucket metadata", err)
		}
		backup.MetadataRows = make([]burnbridgeMetadataRow, 0, len(metadataRows))
		for _, row := range metadataRows {
			copied := make([]byte, len(row.Value))
			copy(copied, row.Value)
			backup.MetadataRows = append(backup.MetadataRows, burnbridgeMetadataRow{
				ObjectName: row.ObjectName,
				Attribute:  row.Attribute,
				Value:      copied,
			})
		}

		var segmentRows []burnbridgeObjectSegment
		if err := db.Where("bucket = ?", trimmedBucket).Order("object_name ASC, segment_index ASC").Find(&segmentRows).Error; err != nil {
			return mapSQLError("export burnbridge bucket segments", err)
		}
		backup.SegmentRows = make([]burnbridgeSegmentRow, 0, len(segmentRows))
		for _, row := range segmentRows {
			backup.SegmentRows = append(backup.SegmentRows, burnbridgeSegmentRow{
				ObjectName:   row.ObjectName,
				SegmentIndex: row.SegmentIndex,
				MediaID:      row.MediaID,
				ByteOffset:   row.ByteOffset,
				ByteSize:     row.ByteSize,
				ChecksumMD5:  row.ChecksumMD5,
				BurnState:    row.BurnState,
				DiscExtents:  row.DiscExtents,
				CreatedAt:    row.CreatedAt,
				UpdatedAt:    row.UpdatedAt,
			})
		}

		var sessionRows []burnbridgeUploadSession
		if err := db.Where("bucket = ?", trimmedBucket).Order("object_name ASC, upload_id ASC").Find(&sessionRows).Error; err != nil {
			return mapSQLError("export burnbridge bucket sessions", err)
		}
		backup.SessionRows = make([]burnbridgeSessionRow, 0, len(sessionRows))
		for _, row := range sessionRows {
			backup.SessionRows = append(backup.SessionRows, burnbridgeSessionRow{
				ObjectName:     row.ObjectName,
				UploadID:       row.UploadID,
				Kind:           row.Kind,
				State:          row.State,
				MediaID:        row.MediaID,
				RecorderJobID:  row.RecorderJobID,
				ContentLength:  row.ContentLength,
				BytesReceived:  row.BytesReceived,
				NextPartNumber: row.NextPartNumber,
				CreatedAt:      row.CreatedAt,
				UpdatedAt:      row.UpdatedAt,
			})
		}

		var partRows []burnbridgeUploadPart
		if err := db.Where("bucket = ?", trimmedBucket).Order("object_name ASC, upload_id ASC, part_number ASC").Find(&partRows).Error; err != nil {
			return mapSQLError("export burnbridge bucket parts", err)
		}
		backup.PartRows = make([]burnbridgePartRow, 0, len(partRows))
		for _, row := range partRows {
			backup.PartRows = append(backup.PartRows, burnbridgePartRow{
				ObjectName:    row.ObjectName,
				UploadID:      row.UploadID,
				PartNumber:    row.PartNumber,
				StartOffset:   row.StartOffset,
				BytesReceived: row.BytesReceived,
				PartSize:      row.PartSize,
				ChecksumMD5:   row.ChecksumMD5,
				ETag:          row.ETag,
				State:         row.State,
				SegmentCount:  row.SegmentCount,
				CreatedAt:     row.CreatedAt,
				UpdatedAt:     row.UpdatedAt,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return backup, nil
}

// ListBurnbridgeBuckets returns every bucket with persisted BurnBridge runtime state.
func (s SqlMeta) ListBurnbridgeBuckets() ([]string, error) {
	buckets := map[string]struct{}{}
	err := s.withDB("list burnbridge buckets", func(db *gorm.DB) error {
		var metadataBuckets []string
		if err := db.Model(&metadataEntry{}).Distinct("bucket").Pluck("bucket", &metadataBuckets).Error; err != nil {
			return mapSQLError("list burnbridge metadata buckets", err)
		}
		for _, bucket := range metadataBuckets {
			if trimmed := strings.TrimSpace(bucket); trimmed != "" {
				buckets[trimmed] = struct{}{}
			}
		}

		var segmentBuckets []string
		if err := db.Model(&burnbridgeObjectSegment{}).Distinct("bucket").Pluck("bucket", &segmentBuckets).Error; err != nil {
			return mapSQLError("list burnbridge segment buckets", err)
		}
		for _, bucket := range segmentBuckets {
			if trimmed := strings.TrimSpace(bucket); trimmed != "" {
				buckets[trimmed] = struct{}{}
			}
		}

		var sessionBuckets []string
		if err := db.Model(&burnbridgeUploadSession{}).Distinct("bucket").Pluck("bucket", &sessionBuckets).Error; err != nil {
			return mapSQLError("list burnbridge session buckets", err)
		}
		for _, bucket := range sessionBuckets {
			if trimmed := strings.TrimSpace(bucket); trimmed != "" {
				buckets[trimmed] = struct{}{}
			}
		}

		var partBuckets []string
		if err := db.Model(&burnbridgeUploadPart{}).Distinct("bucket").Pluck("bucket", &partBuckets).Error; err != nil {
			return mapSQLError("list burnbridge part buckets", err)
		}
		for _, bucket := range partBuckets {
			if trimmed := strings.TrimSpace(bucket); trimmed != "" {
				buckets[trimmed] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, len(buckets))
	for bucket := range buckets {
		out = append(out, bucket)
	}
	sort.Strings(out)
	return out, nil
}

// DeleteBurnbridgeBucket removes all persisted BurnBridge runtime state for a bucket.
func (s SqlMeta) DeleteBurnbridgeBucket(bucket string) error {
	trimmedBucket := strings.TrimSpace(bucket)
	if trimmedBucket == "" {
		return fmt.Errorf("delete burnbridge bucket: empty bucket")
	}

	return s.withDB("delete burnbridge bucket", func(db *gorm.DB) error {
		return db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Where("bucket = ?", trimmedBucket).Delete(&metadataEntry{}).Error; err != nil {
				return mapSQLError("delete burnbridge bucket metadata", err)
			}
			if err := tx.Where("bucket = ?", trimmedBucket).Delete(&burnbridgeObjectSegment{}).Error; err != nil {
				return mapSQLError("delete burnbridge bucket segments", err)
			}
			if err := tx.Where("bucket = ?", trimmedBucket).Delete(&burnbridgeUploadSession{}).Error; err != nil {
				return mapSQLError("delete burnbridge bucket sessions", err)
			}
			if err := tx.Where("bucket = ?", trimmedBucket).Delete(&burnbridgeUploadPart{}).Error; err != nil {
				return mapSQLError("delete burnbridge bucket parts", err)
			}
			return nil
		})
	})
}

func (s SqlMeta) RestoreBurnbridgeBucket(backup *BurnbridgeBucketBackup) error {
	if backup == nil {
		return nil
	}
	trimmedBucket := strings.TrimSpace(backup.Bucket)
	if trimmedBucket == "" {
		return fmt.Errorf("restore burnbridge bucket: empty bucket")
	}

	return s.withDB("restore burnbridge bucket", func(db *gorm.DB) error {
		return db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Where("bucket = ?", trimmedBucket).Delete(&metadataEntry{}).Error; err != nil {
				return mapSQLError("restore burnbridge bucket metadata reset", err)
			}
			if err := tx.Where("bucket = ?", trimmedBucket).Delete(&burnbridgeObjectSegment{}).Error; err != nil {
				return mapSQLError("restore burnbridge bucket segment reset", err)
			}
			if err := tx.Where("bucket = ?", trimmedBucket).Delete(&burnbridgeUploadSession{}).Error; err != nil {
				return mapSQLError("restore burnbridge bucket session reset", err)
			}
			if err := tx.Where("bucket = ?", trimmedBucket).Delete(&burnbridgeUploadPart{}).Error; err != nil {
				return mapSQLError("restore burnbridge bucket part reset", err)
			}

			if len(backup.MetadataRows) > 0 {
				rows := make([]metadataEntry, 0, len(backup.MetadataRows))
				now := time.Now().UTC()
				for _, row := range backup.MetadataRows {
					copied := make([]byte, len(row.Value))
					copy(copied, row.Value)
					rows = append(rows, metadataEntry{
						Bucket:     trimmedBucket,
						ObjectName: row.ObjectName,
						Attribute:  row.Attribute,
						Value:      copied,
						CreatedAt:  now,
						UpdatedAt:  now,
					})
				}
				if err := tx.CreateInBatches(&rows, burnbridgeRestoreBatchSize).Error; err != nil {
					return mapSQLError("restore burnbridge bucket metadata insert", err)
				}
			}

			if len(backup.SegmentRows) > 0 {
				rows := make([]burnbridgeObjectSegment, 0, len(backup.SegmentRows))
				now := time.Now().UTC()
				for _, row := range backup.SegmentRows {
					rows = append(rows, burnbridgeObjectSegment{
						Bucket:       trimmedBucket,
						ObjectName:   row.ObjectName,
						SegmentIndex: row.SegmentIndex,
						MediaID:      row.MediaID,
						ByteOffset:   row.ByteOffset,
						ByteSize:     row.ByteSize,
						ChecksumMD5:  row.ChecksumMD5,
						BurnState:    row.BurnState,
						DiscExtents:  row.DiscExtents,
						CreatedAt:    coalesceTime(row.CreatedAt, now),
						UpdatedAt:    coalesceTime(row.UpdatedAt, now),
					})
				}
				if err := tx.CreateInBatches(&rows, burnbridgeRestoreBatchSize).Error; err != nil {
					return mapSQLError("restore burnbridge bucket segments insert", err)
				}
			}

			if len(backup.SessionRows) > 0 {
				rows := make([]burnbridgeUploadSession, 0, len(backup.SessionRows))
				now := time.Now().UTC()
				for _, row := range backup.SessionRows {
					rows = append(rows, burnbridgeUploadSession{
						Bucket:         trimmedBucket,
						ObjectName:     row.ObjectName,
						UploadID:       row.UploadID,
						Kind:           row.Kind,
						State:          row.State,
						MediaID:        row.MediaID,
						RecorderJobID:  row.RecorderJobID,
						ContentLength:  row.ContentLength,
						BytesReceived:  row.BytesReceived,
						NextPartNumber: row.NextPartNumber,
						CreatedAt:      coalesceTime(row.CreatedAt, now),
						UpdatedAt:      coalesceTime(row.UpdatedAt, now),
					})
				}
				if err := tx.CreateInBatches(&rows, burnbridgeRestoreBatchSize).Error; err != nil {
					return mapSQLError("restore burnbridge bucket sessions insert", err)
				}
			}

			if len(backup.PartRows) > 0 {
				rows := make([]burnbridgeUploadPart, 0, len(backup.PartRows))
				now := time.Now().UTC()
				for _, row := range backup.PartRows {
					rows = append(rows, burnbridgeUploadPart{
						Bucket:        trimmedBucket,
						ObjectName:    row.ObjectName,
						UploadID:      row.UploadID,
						PartNumber:    row.PartNumber,
						StartOffset:   row.StartOffset,
						BytesReceived: row.BytesReceived,
						PartSize:      row.PartSize,
						ChecksumMD5:   row.ChecksumMD5,
						ETag:          row.ETag,
						State:         row.State,
						SegmentCount:  row.SegmentCount,
						CreatedAt:     coalesceTime(row.CreatedAt, now),
						UpdatedAt:     coalesceTime(row.UpdatedAt, now),
					})
				}
				if err := tx.CreateInBatches(&rows, burnbridgeRestoreBatchSize).Error; err != nil {
					return mapSQLError("restore burnbridge bucket parts insert", err)
				}
			}
			return nil
		})
	})
}

func coalesceTime(value time.Time, fallback time.Time) time.Time {
	if value.IsZero() {
		return fallback
	}
	return value
}

// CommittedObjectSummary is one object row derived from SQLite metadata (burnbridge PutObject completion).
type CommittedObjectSummary struct {
	ObjectKey          string
	Size               int64
	LastModified       time.Time
	ETag               string
	ContentType        string
	ContentEncoding    string
	ContentDisposition string
	CacheControl       string
	ContentLanguage    string
	Expires            string
	// Metadata is S3 user metadata (x-amz-meta-* style keys as stored at PutObject).
	Metadata map[string]string
}

// BurnbridgeCommittedAttribute is the metadata_entries key for a JSON snapshot after successful BurnBridge PutObject.
const BurnbridgeCommittedAttribute = "burnbridge-committed"

// BurnbridgeDiscInfoObjectKey is the internal metadata object slot used to persist burnbridge disc JSON.
const BurnbridgeDiscInfoObjectKey = "v1/state/disc-info"

// BurnbridgeDriveInfoObjectKey is the internal metadata object slot used to persist burnbridge drive JSON.
const BurnbridgeDriveInfoObjectKey = "v1/state/drive-info"

// BurnbridgeDiscInfoAttribute stores JSON for BurnbridgeDiscInfoDocument (not a committed object; not listed).
const BurnbridgeDiscInfoAttribute = "burnbridge-disc-info"

// BurnbridgeDriveInfoAttribute stores JSON for BurnbridgeDriveInfoDocument (not a committed object; not listed).
const BurnbridgeDriveInfoAttribute = "burnbridge-drive-info"

// BurnbridgeDiscInfoDocument is the persisted recorder/gateway disc snapshot.
type BurnbridgeDiscInfoDocument struct {
	Bucket                        string `json:"bucket"`
	VolumeLabel                   string `json:"volumeLabel"`
	UpdatedAt                     string `json:"updatedAt"` // RFC3339Nano
	DiscSerialNumberHex           string `json:"discSerialNumberHex,omitempty"`
	TotalCapacityBytes            int64  `json:"totalCapacityBytes,omitempty"`
	FreeCapacityBytes             int64  `json:"freeCapacityBytes,omitempty"`
	UsedCapacityBytes             int64  `json:"usedCapacityBytes,omitempty"`
	WritableCapacityBytes         int64  `json:"writableCapacityBytes,omitempty"`
	FinalizeReserveBytes          int64  `json:"finalizeReserveBytes,omitempty"`
	MediaType                     string `json:"mediaType,omitempty"`
	BlockSizeBytes                int32  `json:"blockSizeBytes,omitempty"`
	TotalBlocks                   int32  `json:"totalBlocks,omitempty"`
	FreeBlocks                    int32  `json:"freeBlocks,omitempty"`
	RecordableCapacityBlocks      int32  `json:"recordableCapacityBlocks,omitempty"`
	TrackNextWritableAddress      int32  `json:"trackNextWritableAddress,omitempty"`
	TrackNextWritableAddressValid bool   `json:"trackNextWritableAddressValid,omitempty"`
	WritableState                 string `json:"writableState,omitempty"`
	DiscStatusName                string `json:"discStatusName,omitempty"`
	SessionIsFinalized            bool   `json:"sessionIsFinalized,omitempty"`
	SessionTempDiscId             string `json:"sessionTempDiscId,omitempty"`
	LayoutStatus                  string `json:"layoutStatus,omitempty"`
	LayoutMessage                 string `json:"layoutMessage,omitempty"`
	LayoutCompletedAtUtc          string `json:"layoutCompletedAtUtc,omitempty"`
	LayoutCloseDisc               bool   `json:"layoutCloseDisc,omitempty"`
}

// BurnbridgeDriveInfoDocument is the persisted recorder/gateway drive identity snapshot.
type BurnbridgeDriveInfoDocument struct {
	Bucket          string `json:"bucket"`
	ControlBucket   string `json:"controlBucket"`
	UpdatedAt       string `json:"updatedAt"` // RFC3339Nano
	VendorID        string `json:"vendorId,omitempty"`
	ProductID       string `json:"productId,omitempty"`
	ProductRevision string `json:"productRevision,omitempty"`
	SerialNumber    string `json:"serialNumber,omitempty"`
	IsMMCUnit       bool   `json:"isMmcUnit,omitempty"`
}

// StoreBurnbridgeDiscInfo upserts disc JSON into the internal burnbridge control slot.
func (s SqlMeta) StoreBurnbridgeDiscInfo(doc *BurnbridgeDiscInfoDocument) error {
	if doc == nil {
		return nil
	}
	if strings.TrimSpace(doc.Bucket) == "" {
		return fmt.Errorf("burnbridge disc info: empty bucket")
	}
	b, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("encode burnbridge disc info: %w", err)
	}
	return s.StoreAttribute(nil, doc.Bucket, BurnbridgeDiscInfoObjectKey, BurnbridgeDiscInfoAttribute, b)
}

// StoreBurnbridgeDriveInfo upserts drive JSON into the internal burnbridge control slot.
func (s SqlMeta) StoreBurnbridgeDriveInfo(doc *BurnbridgeDriveInfoDocument) error {
	if doc == nil {
		return nil
	}
	if strings.TrimSpace(doc.ControlBucket) == "" {
		return fmt.Errorf("burnbridge drive info: empty control bucket")
	}
	bucket := strings.TrimSpace(doc.Bucket)
	if bucket == "" {
		bucket = strings.TrimSpace(doc.ControlBucket)
	}
	b, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("encode burnbridge drive info: %w", err)
	}
	return s.StoreAttribute(nil, bucket, BurnbridgeDriveInfoObjectKey, BurnbridgeDriveInfoAttribute, b)
}

// GetBurnbridgeDriveInfoJSON returns raw persisted drive JSON.
func (s SqlMeta) GetBurnbridgeDriveInfoJSON(bucket string) ([]byte, error) {
	return s.RetrieveAttribute(nil, bucket, BurnbridgeDriveInfoObjectKey, BurnbridgeDriveInfoAttribute)
}

// GetBurnbridgeDiscInfoJSON returns raw persisted disc JSON.
func (s SqlMeta) GetBurnbridgeDiscInfoJSON(bucket string) ([]byte, error) {
	return s.RetrieveAttribute(nil, bucket, BurnbridgeDiscInfoObjectKey, BurnbridgeDiscInfoAttribute)
}

// BurnbridgeFinalizeLayoutObjectKey is the internal metadata object slot for finalize transcript caching.
const BurnbridgeFinalizeLayoutObjectKey = "v1/state/finalize-layout"

// BurnbridgeCloseDiscObjectKey is the internal metadata object slot for close-disc transcript caching.
const BurnbridgeCloseDiscObjectKey = "v1/state/close-disc"

// BurnbridgeForceCloseDiscObjectKey is the internal metadata object slot for force close-disc transcript caching.
const BurnbridgeForceCloseDiscObjectKey = "v1/state/close-disc-force"

// BurnbridgeMediaRemovedObjectKey is the internal metadata object slot for media removal transcript caching.
const BurnbridgeMediaRemovedObjectKey = "v1/state/media-removed"

// BurnbridgeMediaInsertedObjectKey is the internal metadata object slot for media insertion transcript caching.
const BurnbridgeMediaInsertedObjectKey = "v1/state/media-inserted"

// BurnbridgeTrayOpenObjectKey is the internal metadata object slot for tray-open transcript caching.
const BurnbridgeTrayOpenObjectKey = "v1/state/tray-open"

// BurnbridgeTrayCloseObjectKey is the internal metadata object slot for tray-close transcript caching.
const BurnbridgeTrayCloseObjectKey = "v1/state/tray-close"

// BurnbridgeFinalizeLayoutAttributePrefix holds JSON documenting the last finalize-style gRPC invocation.
const BurnbridgeFinalizeLayoutAttributePrefix = "burnbridge-finalize-layout"

// BurnbridgeMediaChangeAttributePrefix holds JSON documenting the last media-change gRPC invocation.
const BurnbridgeMediaChangeAttributePrefix = "burnbridge-media-change"

// BurnbridgeTrayAttributePrefix holds JSON documenting the last tray-control gRPC invocation.
const BurnbridgeTrayAttributePrefix = "burnbridge-tray"

// BurnbridgeFinalizeLayoutDocument captures the outcome of invoking the recorder finalize RPC (from gateway).
type BurnbridgeFinalizeLayoutDocument struct {
	Bucket                  string                                        `json:"bucket"`
	RequestID               string                                        `json:"requestId,omitempty"`
	RequestTime             int64                                         `json:"requestTime,omitempty"`
	RecorderStatus          string                                        `json:"recorderStatus"`
	RecorderMessage         string                                        `json:"recorderMessage,omitempty"`
	CloseDisc               bool                                          `json:"closeDisc,omitempty"`
	Force                   bool                                          `json:"force,omitempty"`
	StagedFlushError        string                                        `json:"stagedFlushError,omitempty"`
	Discarded               []BurnbridgeForceCloseDiscardedObjectDocument `json:"discarded,omitempty"`
	DiscardedUploadSessions int                                           `json:"discardedUploadSessions,omitempty"`
	CompletedAtUtc          string                                        `json:"completedAtUtc"` // RFC3339Nano when the gateway persisted this record
	GrpcOK                  bool                                          `json:"grpcOk"`
	GrpcCode                string                                        `json:"grpcCode,omitempty"`
	GrpcDetails             string                                        `json:"grpcDetails,omitempty"`
	Error                   string                                        `json:"error,omitempty"`
}

// BurnbridgeForceCloseDiscardedObjectDocument records metadata removed by a force close-disc operation.
type BurnbridgeForceCloseDiscardedObjectDocument struct {
	ObjectKey string `json:"objectKey"`
	Reason    string `json:"reason"`
	Size      int64  `json:"size,omitempty"`
	ETag      string `json:"etag,omitempty"`
	UploadID  string `json:"uploadId,omitempty"`
	JobID     string `json:"jobId,omitempty"`
}

// BurnbridgeMediaChangeDocument captures the outcome of invoking the recorder media-change RPC (from gateway).
type BurnbridgeMediaChangeDocument struct {
	Bucket          string `json:"bucket"`
	RequestID       string `json:"requestId,omitempty"`
	RequestTime     int64  `json:"requestTime,omitempty"`
	Action          string `json:"action"`
	RecorderStatus  string `json:"recorderStatus"`
	RecorderMessage string `json:"recorderMessage,omitempty"`
	ImportedBucket  string `json:"importedBucket,omitempty"`
	Ready           bool   `json:"ready,omitempty"`
	VolumeLabel     string `json:"volumeLabel,omitempty"`
	WritableState   string `json:"writableState,omitempty"`
	CompletedAtUtc  string `json:"completedAtUtc"`
	GrpcOK          bool   `json:"grpcOk"`
	GrpcCode        string `json:"grpcCode,omitempty"`
	GrpcDetails     string `json:"grpcDetails,omitempty"`
	Error           string `json:"error,omitempty"`
}

// BurnbridgeTrayDocument captures the outcome of invoking the recorder tray RPC (from gateway).
type BurnbridgeTrayDocument struct {
	Bucket          string `json:"bucket"`
	RequestID       string `json:"requestId,omitempty"`
	RequestTime     int64  `json:"requestTime,omitempty"`
	Action          string `json:"action"`
	RecorderStatus  string `json:"recorderStatus"`
	RecorderMessage string `json:"recorderMessage,omitempty"`
	CompletedAtUtc  string `json:"completedAtUtc"`
	GrpcOK          bool   `json:"grpcOk"`
	GrpcCode        string `json:"grpcCode,omitempty"`
	GrpcDetails     string `json:"grpcDetails,omitempty"`
	Error           string `json:"error,omitempty"`
}

// BurnbridgeRuntimeBindingBucket is a reserved internal metadata bucket used for runtime-only optical media bindings.
const BurnbridgeRuntimeBindingBucket = "__burnbridge_runtime__"

// BurnbridgeDiscBucketBindingAttribute stores the disc probe label -> logical bucket/UDF label mapping.
const BurnbridgeDiscBucketBindingAttribute = "burnbridge-disc-bucket-binding"

// BurnbridgeDiscBucketBindingDocument is persisted in gateway SQLite metadata and must not be stored in config files.
type BurnbridgeDiscBucketBindingDocument struct {
	ProbeVolumeLabel string `json:"probeVolumeLabel"`
	Bucket           string `json:"bucket"`
	UdfVolumeLabel   string `json:"udfVolumeLabel"`
	UpdatedAtUtc     string `json:"updatedAtUtc"`
}

func burnbridgeFinalizeLayoutAttributeForObjectKey(objectKey string) (string, error) {
	switch strings.TrimSpace(objectKey) {
	case BurnbridgeFinalizeLayoutObjectKey:
		return BurnbridgeFinalizeLayoutAttributePrefix, nil
	case BurnbridgeCloseDiscObjectKey:
		return BurnbridgeFinalizeLayoutAttributePrefix + "-close-disc", nil
	case BurnbridgeForceCloseDiscObjectKey:
		return BurnbridgeFinalizeLayoutAttributePrefix + "-close-disc-force", nil
	default:
		return "", fmt.Errorf("finalize layout: unsupported object key %q", objectKey)
	}
}

func burnbridgeMediaChangeAttributeForObjectKey(objectKey string) (string, error) {
	switch strings.TrimSpace(objectKey) {
	case BurnbridgeMediaRemovedObjectKey:
		return BurnbridgeMediaChangeAttributePrefix + "-removed", nil
	case BurnbridgeMediaInsertedObjectKey:
		return BurnbridgeMediaChangeAttributePrefix + "-inserted", nil
	default:
		return "", fmt.Errorf("media change: unsupported object key %q", objectKey)
	}
}

func burnbridgeTrayAttributeForObjectKey(objectKey string) (string, error) {
	switch strings.TrimSpace(objectKey) {
	case BurnbridgeTrayOpenObjectKey:
		return BurnbridgeTrayAttributePrefix + "-open", nil
	case BurnbridgeTrayCloseObjectKey:
		return BurnbridgeTrayAttributePrefix + "-close", nil
	default:
		return "", fmt.Errorf("tray control: unsupported object key %q", objectKey)
	}
}

// StoreBurnbridgeFinalizeLayoutJSON saves the finalize outcome for an internal finalize-style control slot.
func (s SqlMeta) StoreBurnbridgeFinalizeLayoutJSON(bucket string, objectKey string, payload []byte) error {
	if strings.TrimSpace(bucket) == "" {
		return fmt.Errorf("finalize layout: empty bucket")
	}
	attr, err := burnbridgeFinalizeLayoutAttributeForObjectKey(objectKey)
	if err != nil {
		return err
	}
	if len(payload) == 0 {
		return fmt.Errorf("finalize layout: empty payload")
	}
	return s.StoreAttribute(nil, bucket, objectKey, attr, payload)
}

// StoreBurnbridgeMediaChangeJSON saves the media-change outcome for an internal control slot.
func (s SqlMeta) StoreBurnbridgeMediaChangeJSON(bucket string, objectKey string, payload []byte) error {
	if strings.TrimSpace(bucket) == "" {
		return fmt.Errorf("media change: empty bucket")
	}
	attr, err := burnbridgeMediaChangeAttributeForObjectKey(objectKey)
	if err != nil {
		return err
	}
	return s.StoreAttribute(nil, bucket, objectKey, attr, payload)
}

// GetBurnbridgeMediaChangeJSON returns cached media-change JSON for a control slot.
func (s SqlMeta) GetBurnbridgeMediaChangeJSON(bucket string, objectKey string) ([]byte, error) {
	attr, err := burnbridgeMediaChangeAttributeForObjectKey(objectKey)
	if err != nil {
		return nil, err
	}
	return s.RetrieveAttribute(nil, bucket, objectKey, attr)
}

// StoreBurnbridgeTrayJSON saves the tray-control outcome for an internal control slot.
func (s SqlMeta) StoreBurnbridgeTrayJSON(bucket string, objectKey string, payload []byte) error {
	if strings.TrimSpace(bucket) == "" {
		return fmt.Errorf("tray control: empty bucket")
	}
	attr, err := burnbridgeTrayAttributeForObjectKey(objectKey)
	if err != nil {
		return err
	}
	return s.StoreAttribute(nil, bucket, objectKey, attr, payload)
}

// GetBurnbridgeTrayJSON returns cached tray-control JSON for a control slot.
func (s SqlMeta) GetBurnbridgeTrayJSON(bucket string, objectKey string) ([]byte, error) {
	attr, err := burnbridgeTrayAttributeForObjectKey(objectKey)
	if err != nil {
		return nil, err
	}
	return s.RetrieveAttribute(nil, bucket, objectKey, attr)
}

// GetBurnbridgeFinalizeLayoutJSON returns raw JSON persisted for an internal finalize-style control slot.
func (s SqlMeta) GetBurnbridgeFinalizeLayoutJSON(bucket string, objectKey string) ([]byte, error) {
	attr, err := burnbridgeFinalizeLayoutAttributeForObjectKey(objectKey)
	if err != nil {
		return nil, err
	}
	return s.RetrieveAttribute(nil, bucket, objectKey, attr)
}

// DeleteBurnbridgeFinalizeLayoutJSON removes the cached finalize transcript for an internal finalize-style control slot.
func (s SqlMeta) DeleteBurnbridgeFinalizeLayoutJSON(bucket string, objectKey string) error {
	attr, err := burnbridgeFinalizeLayoutAttributeForObjectKey(objectKey)
	if err != nil {
		return err
	}
	return s.DeleteAttribute(bucket, objectKey, attr)
}

// StoreBurnbridgeDiscBucketBinding persists runtime media naming for restart-safe blank-disc bucket binding.
func (s SqlMeta) StoreBurnbridgeDiscBucketBinding(doc *BurnbridgeDiscBucketBindingDocument) error {
	if doc == nil {
		return nil
	}
	probe := strings.TrimSpace(doc.ProbeVolumeLabel)
	bucket := strings.TrimSpace(doc.Bucket)
	udf := strings.TrimSpace(doc.UdfVolumeLabel)
	if probe == "" {
		return fmt.Errorf("disc bucket binding: empty probe volume label")
	}
	if bucket == "" {
		return fmt.Errorf("disc bucket binding: empty bucket")
	}
	if udf == "" {
		return fmt.Errorf("disc bucket binding: empty udf volume label")
	}

	normalized := BurnbridgeDiscBucketBindingDocument{
		ProbeVolumeLabel: probe,
		Bucket:           bucket,
		UdfVolumeLabel:   udf,
		UpdatedAtUtc:     doc.UpdatedAtUtc,
	}
	if strings.TrimSpace(normalized.UpdatedAtUtc) == "" {
		normalized.UpdatedAtUtc = time.Now().UTC().Format(time.RFC3339Nano)
	}

	payload, err := json.Marshal(&normalized)
	if err != nil {
		return fmt.Errorf("encode disc bucket binding: %w", err)
	}
	return s.StoreAttribute(nil, BurnbridgeRuntimeBindingBucket, probe, BurnbridgeDiscBucketBindingAttribute, payload)
}

// GetBurnbridgeDiscBucketBinding returns a persisted runtime media binding by recorder probe volume label.
func (s SqlMeta) GetBurnbridgeDiscBucketBinding(probeVolumeLabel string) (*BurnbridgeDiscBucketBindingDocument, error) {
	probe := strings.TrimSpace(probeVolumeLabel)
	if probe == "" {
		return nil, ErrNoSuchKey
	}

	raw, err := s.RetrieveAttribute(nil, BurnbridgeRuntimeBindingBucket, probe, BurnbridgeDiscBucketBindingAttribute)
	if err != nil {
		return nil, err
	}

	var doc BurnbridgeDiscBucketBindingDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("decode disc bucket binding: %w", err)
	}
	return &doc, nil
}

// ListBurnbridgeDiscBucketBindings returns persisted runtime media bindings.
// When bucket is non-empty, only bindings for that logical bucket are returned.
func (s SqlMeta) ListBurnbridgeDiscBucketBindings(bucket string) ([]BurnbridgeDiscBucketBindingDocument, error) {
	var out []BurnbridgeDiscBucketBindingDocument
	err := s.withDB("list disc bucket bindings", func(db *gorm.DB) error {
		query := db.
			Table("metadata_entries").
			Select("value").
			Where("bucket = ? AND attribute = ?", BurnbridgeRuntimeBindingBucket, BurnbridgeDiscBucketBindingAttribute)
		if trimmed := strings.TrimSpace(bucket); trimmed != "" {
			query = query.Where("json_extract(value, '$.bucket') = ?", trimmed)
		}

		rows, err := query.Rows()
		if err != nil {
			return mapSQLError("list disc bucket bindings", err)
		}
		defer rows.Close()

		var docs []BurnbridgeDiscBucketBindingDocument
		for rows.Next() {
			var raw []byte
			if err := rows.Scan(&raw); err != nil {
				return mapSQLError("scan disc bucket binding", err)
			}

			var doc BurnbridgeDiscBucketBindingDocument
			if err := json.Unmarshal(raw, &doc); err != nil {
				return fmt.Errorf("decode disc bucket binding: %w", err)
			}
			docs = append(docs, doc)
		}
		if err := rows.Err(); err != nil {
			return mapSQLError("list disc bucket bindings", err)
		}

		out = docs
		return nil
	})
	return out, err
}

// DeleteBurnbridgeDiscBucketBindings removes runtime media bindings for a logical bucket.
func (s SqlMeta) DeleteBurnbridgeDiscBucketBindings(bucket string) error {
	trimmedBucket := strings.TrimSpace(bucket)
	if trimmedBucket == "" {
		return nil
	}

	return s.withDB("delete disc bucket bindings", func(db *gorm.DB) error {
		if err := db.
			Where("bucket = ? AND attribute = ? AND json_extract(value, '$.bucket') = ?",
				BurnbridgeRuntimeBindingBucket,
				BurnbridgeDiscBucketBindingAttribute,
				trimmedBucket).
			Delete(&metadataEntry{}).Error; err != nil {
			return mapSQLError("delete disc bucket bindings", err)
		}
		return nil
	})
}

// BurnbridgeCommittedRecord is JSON-encoded into metadata_entries under BurnbridgeCommittedAttribute.
type BurnbridgeCommittedRecord struct {
	JobID              string            `json:"jobId,omitempty"`
	Status             string            `json:"status,omitempty"`
	ETag               string            `json:"etag,omitempty"`
	LastModified       string            `json:"lastModified,omitempty"` // RFC3339Nano
	Size               int64             `json:"size"`
	ContentType        string            `json:"contentType,omitempty"`
	ContentEncoding    string            `json:"contentEncoding,omitempty"`
	ContentDisposition string            `json:"contentDisposition,omitempty"`
	ContentLanguage    string            `json:"contentLanguage,omitempty"`
	CacheControl       string            `json:"cacheControl,omitempty"`
	Expires            string            `json:"expires,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
}

func committedSummaryFromJSON(objectName string, raw []byte) (CommittedObjectSummary, error) {
	var rec BurnbridgeCommittedRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return CommittedObjectSummary{}, fmt.Errorf("decode burnbridge committed json: %w", err)
	}
	return SummaryFromBurnbridgeCommittedRecord(&rec, objectName), nil
}

// SummaryFromBurnbridgeCommittedRecord maps a decoded BurnbridgeCommittedRecord to CommittedObjectSummary.
func SummaryFromBurnbridgeCommittedRecord(rec *BurnbridgeCommittedRecord, objectKey string) CommittedObjectSummary {
	if rec == nil {
		return CommittedObjectSummary{ObjectKey: objectKey, LastModified: time.Now().UTC()}
	}
	return rec.committedObjectSummary(objectKey)
}

func (rec *BurnbridgeCommittedRecord) committedObjectSummary(objectName string) CommittedObjectSummary {
	if rec == nil {
		return CommittedObjectSummary{ObjectKey: objectName, LastModified: time.Now().UTC()}
	}
	sum := CommittedObjectSummary{
		ObjectKey:          objectName,
		Size:               rec.Size,
		ETag:               rec.ETag,
		ContentType:        rec.ContentType,
		ContentEncoding:    rec.ContentEncoding,
		ContentDisposition: rec.ContentDisposition,
		CacheControl:       rec.CacheControl,
		ContentLanguage:    rec.ContentLanguage,
		Expires:            rec.Expires,
	}
	if len(rec.Metadata) > 0 {
		sum.Metadata = make(map[string]string, len(rec.Metadata))
		for k, v := range rec.Metadata {
			sum.Metadata[k] = v
		}
	}
	if rec.LastModified != "" {
		if t, err := time.Parse(time.RFC3339Nano, rec.LastModified); err == nil {
			sum.LastModified = t
		} else if t, err := time.Parse(time.RFC3339, rec.LastModified); err == nil {
			sum.LastModified = t
		}
	}
	if sum.LastModified.IsZero() {
		sum.LastModified = time.Now().UTC()
	}
	return sum
}

// GetBurnbridgeCommittedRecord loads the full committed JSON for an object. ErrNoSuchKey if absent.
func (s SqlMeta) GetBurnbridgeCommittedRecord(bucket, objectKey string) (*BurnbridgeCommittedRecord, error) {
	raw, err := s.RetrieveAttribute(nil, bucket, objectKey, BurnbridgeCommittedAttribute)
	if err != nil {
		return nil, err
	}
	var rec BurnbridgeCommittedRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, fmt.Errorf("decode burnbridge committed json: %w", err)
	}
	return &rec, nil
}

// StoreBurnbridgeCommitted writes one JSON blob for completed put (job id, etag, headers, size).
func (s SqlMeta) StoreBurnbridgeCommitted(_ *os.File, bucket, object string, rec *BurnbridgeCommittedRecord) error {
	if rec == nil {
		return fmt.Errorf("burnbridge committed record is nil")
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("encode burnbridge committed json: %w", err)
	}
	return s.StoreAttribute(nil, bucket, object, BurnbridgeCommittedAttribute, b)
}

// ListCommittedObjects returns summaries for objects that have burnbridge committed JSON, ordered by object_name.
func (s SqlMeta) ListCommittedObjects(bucket string) ([]CommittedObjectSummary, error) {
	var out []CommittedObjectSummary
	err := s.withDB("list committed objects", func(db *gorm.DB) error {
		var rowsOut []CommittedObjectSummary
		rows, err := db.Raw(
			`SELECT object_name, value FROM metadata_entries WHERE bucket = ? AND attribute = ? ORDER BY object_name`,
			bucket,
			BurnbridgeCommittedAttribute,
		).Rows()
		if err != nil {
			return mapSQLError("list committed objects", err)
		}
		defer rows.Close()
		for rows.Next() {
			var name sql.NullString
			var raw []byte
			if err := rows.Scan(&name, &raw); err != nil {
				return mapSQLError("scan committed object", err)
			}
			if !name.Valid {
				continue
			}
			sum, err := committedSummaryFromJSON(name.String, raw)
			if err != nil {
				return err
			}
			rowsOut = append(rowsOut, sum)
		}
		if err := rows.Err(); err != nil {
			return mapSQLError("list committed objects", err)
		}
		out = rowsOut
		return nil
	})
	return out, err
}

// PruneBurnbridgeCommitted removes committed object metadata and segment rows not present in keepObjectKeys.
func (s SqlMeta) PruneBurnbridgeCommitted(bucket string, keepObjectKeys map[string]struct{}) ([]string, error) {
	existing, err := s.ListCommittedObjects(bucket)
	if err != nil {
		return nil, err
	}
	var removed []string
	for _, sum := range existing {
		key := strings.TrimPrefix(strings.ReplaceAll(sum.ObjectKey, `\`, `/`), "/")
		if _, keep := keepObjectKeys[key]; keep {
			continue
		}
		if err := s.DeleteAttribute(bucket, sum.ObjectKey, BurnbridgeCommittedAttribute); err != nil && !errors.Is(err, ErrNoSuchKey) {
			return removed, err
		}
		if err := s.DeleteBurnObjectSegments(bucket, sum.ObjectKey); err != nil {
			return removed, err
		}
		removed = append(removed, sum.ObjectKey)
	}
	return removed, nil
}

// GetCommittedObjectSummary loads summary for one object. ErrNoSuchKey if no committed JSON row.
func (s SqlMeta) GetCommittedObjectSummary(bucket, objectKey string) (CommittedObjectSummary, error) {
	raw, err := s.RetrieveAttribute(nil, bucket, objectKey, BurnbridgeCommittedAttribute)
	if err != nil {
		return CommittedObjectSummary{}, err
	}
	return committedSummaryFromJSON(objectKey, raw)
}

func (s SqlMeta) RetrieveAttribute(_ *os.File, bucket, object, attribute string) ([]byte, error) {
	var entry metadataEntry
	err := s.withDB("retrieve attribute", func(db *gorm.DB) error {
		err := db.
			Where("bucket = ? AND object_name = ? AND attribute = ?", bucket, object, attribute).
			First(&entry).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNoSuchKey
			}
			return fmt.Errorf("retrieve attribute: %w", err)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrNoSuchKey) {
			return nil, ErrNoSuchKey
		}
		return nil, err
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
	return s.withDB("store attribute", func(db *gorm.DB) error {
		if err := db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "bucket"}, {Name: "object_name"}, {Name: "attribute"}},
			DoUpdates: clause.AssignmentColumns([]string{"value", "updated_at"}),
		}).Create(&entry).Error; err != nil {
			return mapSQLError("store attribute", err)
		}
		return nil
	})
}

func (s SqlMeta) DeleteAttribute(bucket, object, attribute string) error {
	return s.withDB("delete attribute", func(db *gorm.DB) error {
		res := db.Where("bucket = ? AND object_name = ? AND attribute = ?", bucket, object, attribute).Delete(&metadataEntry{})
		if res.Error != nil {
			return mapSQLError("delete attribute", res.Error)
		}
		if res.RowsAffected == 0 {
			return ErrNoSuchKey
		}
		return nil
	})
}

func (s SqlMeta) ListAttributes(bucket, object string) ([]string, error) {
	var attrs []string
	err := s.withDB("list attributes", func(db *gorm.DB) error {
		if err := db.Model(&metadataEntry{}).
			Where("bucket = ? AND object_name = ?", bucket, object).
			Pluck("attribute", &attrs).Error; err != nil {
			return mapSQLError("list attributes", err)
		}
		return nil
	})
	return attrs, err
}

func (s SqlMeta) DeleteAttributes(bucket, object string) error {
	return s.withDB("delete attributes", func(db *gorm.DB) error {
		if object == "" {
			if err := db.Where("bucket = ?", bucket).Delete(&metadataEntry{}).Error; err != nil {
				return mapSQLError("delete bucket attributes", err)
			}
			return nil
		}

		if err := db.Where("bucket = ? AND object_name = ?", bucket, object).Delete(&metadataEntry{}).Error; err != nil {
			return mapSQLError("delete attributes", err)
		}
		return nil
	})
}

func (s SqlMeta) RenameObject(bucket, oldObject, newObject string) error {
	return s.withDB("rename object", func(db *gorm.DB) error {
		return db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(&metadataEntry{}).
				Where("bucket = ? AND object_name = ?", bucket, oldObject).
				Update("object_name", newObject).Error; err != nil {
				return mapSQLError("rename object metadata", err)
			}

			if err := tx.Model(&burnbridgeObjectSegment{}).
				Where("bucket = ? AND object_name = ?", bucket, oldObject).
				Update("object_name", newObject).Error; err != nil {
				return mapSQLError("rename burn segments", err)
			}

			return nil
		})
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
