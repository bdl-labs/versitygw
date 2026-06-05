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
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/versity/versitygw/s3err"
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

// BurnObjectSegment is returned by GetBurnObjectSegment for BurnBridge chunk lookups.
type BurnObjectSegment struct {
	MediaID      string
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

type BurnbridgeBucketBackup struct {
	Bucket         string                     `json:"bucket"`
	BackedUpAtUtc  string                     `json:"backedUpAtUtc"`
	MetadataRows   []burnbridgeMetadataRow    `json:"metadataRows"`
	SegmentRows    []burnbridgeSegmentRow     `json:"segmentRows"`
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
	applySQLiteFlashPragmas(sqlDB, log)

	sqlDB.SetMaxOpenConns(4)
	sqlDB.SetMaxIdleConns(4)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)

	if err := sqliteWithRetry("auto migrate", func() error {
		return db.AutoMigrate(&metadataEntry{}, &burnbridgeObjectSegment{})
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
			MediaID:      row.MediaID,
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
					MediaID:      row.MediaID,
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
		return nil
	})
	if err != nil {
		return nil, err
	}

	return backup, nil
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
				if err := tx.Create(&rows).Error; err != nil {
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
				if err := tx.Create(&rows).Error; err != nil {
					return mapSQLError("restore burnbridge bucket segments insert", err)
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

// BurnbridgeDiscInfoObjectKey is the fixed S3 object key for optical disc metadata (hidden from ListObjects).
const BurnbridgeDiscInfoObjectKey = "DiscInfo"

// BurnbridgeDiscInfoAttribute stores JSON for BurnbridgeDiscInfoDocument (not a committed object; not listed).
const BurnbridgeDiscInfoAttribute = "burnbridge-disc-info"

// BurnbridgeDiscInfoDocument is JSON returned by GetObject/HeadObject for key BurnbridgeDiscInfoObjectKey.
type BurnbridgeDiscInfoDocument struct {
	Bucket                   string `json:"bucket"`
	VolumeLabel              string `json:"volumeLabel"`
	UpdatedAt                string `json:"updatedAt"` // RFC3339Nano
	DiscSerialNumberHex      string `json:"discSerialNumberHex,omitempty"`
	TotalCapacityBytes       int64  `json:"totalCapacityBytes,omitempty"`
	FreeCapacityBytes        int64  `json:"freeCapacityBytes,omitempty"`
	UsedCapacityBytes        int64  `json:"usedCapacityBytes,omitempty"`
	WritableCapacityBytes    int64  `json:"writableCapacityBytes,omitempty"`
	FinalizeReserveBytes     int64  `json:"finalizeReserveBytes,omitempty"`
	MediaType                string `json:"mediaType,omitempty"`
	BlockSizeBytes           int32  `json:"blockSizeBytes,omitempty"`
	TotalBlocks              int32  `json:"totalBlocks,omitempty"`
	FreeBlocks               int32  `json:"freeBlocks,omitempty"`
	RecordableCapacityBlocks int32  `json:"recordableCapacityBlocks,omitempty"`
	TrackNextWritableAddress int32  `json:"trackNextWritableAddress,omitempty"`
	TrackNextWritableAddressValid bool `json:"trackNextWritableAddressValid,omitempty"`
	WritableState            string `json:"writableState,omitempty"`
	DiscStatusName           string `json:"discStatusName,omitempty"`
	SessionIsFinalized       bool   `json:"sessionIsFinalized,omitempty"`
	SessionTempDiscId        string `json:"sessionTempDiscId,omitempty"`
	LayoutStatus             string `json:"layoutStatus,omitempty"`
	LayoutMessage            string `json:"layoutMessage,omitempty"`
	LayoutCompletedAtUtc     string `json:"layoutCompletedAtUtc,omitempty"`
	LayoutCloseDisc          bool   `json:"layoutCloseDisc,omitempty"`
}

// StoreBurnbridgeDiscInfo upserts disc JSON for the reserved DiscInfo key (not visible in ListObjects).
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

// GetBurnbridgeDiscInfoJSON returns raw JSON bytes for GetObject/HeadObject on BurnbridgeDiscInfoObjectKey.
func (s SqlMeta) GetBurnbridgeDiscInfoJSON(bucket string) ([]byte, error) {
	return s.RetrieveAttribute(nil, bucket, BurnbridgeDiscInfoObjectKey, BurnbridgeDiscInfoAttribute)
}

// BurnbridgeFinalizeLayoutObjectKey triggers UDF/disc finalization via GetObject against the recorder.
const BurnbridgeFinalizeLayoutObjectKey = "FinalizeLayout"

// BurnbridgeFinalizeLayoutAttribute holds JSON documenting the last FinalizeLayout gRPC invocation.
const BurnbridgeFinalizeLayoutAttribute = "burnbridge-finalize-layout"

// BurnbridgeFinalizeLayoutDocument captures the outcome of invoking the recorder finalize RPC (from gateway).
type BurnbridgeFinalizeLayoutDocument struct {
	Bucket          string `json:"bucket"`
	RecorderStatus  string `json:"recorderStatus"`
	RecorderMessage string `json:"recorderMessage,omitempty"`
	CloseDisc       bool   `json:"closeDisc,omitempty"`
	CompletedAtUtc  string `json:"completedAtUtc"` // RFC3339Nano when the gateway persisted this record
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

// StoreBurnbridgeFinalizeLayoutJSON saves the finalize outcome for reserved key BurnbridgeFinalizeLayoutObjectKey.
func (s SqlMeta) StoreBurnbridgeFinalizeLayoutJSON(bucket string, payload []byte) error {
	if strings.TrimSpace(bucket) == "" {
		return fmt.Errorf("finalize layout: empty bucket")
	}
	if len(payload) == 0 {
		return fmt.Errorf("finalize layout: empty payload")
	}
	return s.StoreAttribute(nil, bucket, BurnbridgeFinalizeLayoutObjectKey, BurnbridgeFinalizeLayoutAttribute, payload)
}

// GetBurnbridgeFinalizeLayoutJSON returns raw JSON persisted for BurnbridgeFinalizeLayoutObjectKey (if HeadObject/GetObject before first GET finalized, returns ErrNoSuchKey).
func (s SqlMeta) GetBurnbridgeFinalizeLayoutJSON(bucket string) ([]byte, error) {
	return s.RetrieveAttribute(nil, bucket, BurnbridgeFinalizeLayoutObjectKey, BurnbridgeFinalizeLayoutAttribute)
}

// DeleteBurnbridgeFinalizeLayoutJSON removes the cached finalize transcript for the reserved FinalizeLayout key.
func (s SqlMeta) DeleteBurnbridgeFinalizeLayoutJSON(bucket string) error {
	return s.DeleteAttribute(bucket, BurnbridgeFinalizeLayoutObjectKey, BurnbridgeFinalizeLayoutAttribute)
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
