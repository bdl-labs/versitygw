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
	ByteOffset  int64
	ByteSize    int64
	ChecksumMD5 string
	State       BurnSegmentState
	DiscExtents []BurnDiscExtent
}

// Burned reports whether this segment is known to have been placed successfully on media.
func (s BurnObjectSegment) Burned() bool { return s.State == BurnSegmentSucceeded }

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

	if err := db.AutoMigrate(&metadataEntry{}, &burnbridgeObjectSegment{}); err != nil {
		return SqlMeta{}, fmt.Errorf("migrate sqlite schema: %w", err)
	}
	if db.Migrator().HasColumn(&burnbridgeObjectSegment{}, "burned") {
		_ = db.Exec("UPDATE burnbridge_object_segments SET burn_state = ? WHERE burned = ?", BurnSegmentSucceeded, true).Error
	}

	return SqlMeta{db: db}, nil
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
	var row burnbridgeObjectSegment
	err := s.db.Where("bucket = ? AND object_name = ? AND segment_index = ?", bucket, object, segmentIndex).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return BurnObjectSegment{}, ErrNoSuchKey
		}
		return BurnObjectSegment{}, mapSQLError("get burn object segment", err)
	}
	extents, perr := parseDiscExtentsJSON(row.DiscExtents)
	if perr != nil {
		return BurnObjectSegment{}, perr
	}
	return BurnObjectSegment{
		ByteOffset:  row.ByteOffset,
		ByteSize:    row.ByteSize,
		ChecksumMD5: row.ChecksumMD5,
		State:       BurnSegmentState(row.BurnState),
		DiscExtents: extents,
	}, nil
}

// UpsertBurnObjectSegment inserts or replaces per-segment metadata (pending, succeeded, or failed).
// extents may be nil or empty if the recorder has not reported on-media placement yet; stored as JSON [].
func (s SqlMeta) UpsertBurnObjectSegment(bucket, object string, segmentIndex int, byteOffset, byteSize int64, checksumMD5 string, state BurnSegmentState, extents []BurnDiscExtent) error {
	discJSON, err := marshalDiscExtentsJSON(extents)
	if err != nil {
		return fmt.Errorf("burn segment disc_extents: %w", err)
	}
	row := burnbridgeObjectSegment{
		Bucket:       bucket,
		ObjectName:   object,
		SegmentIndex: segmentIndex,
		ByteOffset:   byteOffset,
		ByteSize:     byteSize,
		ChecksumMD5:  checksumMD5,
		BurnState:    int(state),
		DiscExtents:  discJSON,
	}
	if err := s.db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "bucket"}, {Name: "object_name"}, {Name: "segment_index"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"byte_offset", "byte_size", "checksum_md5", "burn_state", "disc_extents", "updated_at",
		}),
	}).Create(&row).Error; err != nil {
		return mapSQLError("upsert burn object segment", err)
	}
	return nil
}

// DeleteBurnObjectSegments removes all chunk rows for an object (e.g. content changed at segment 0).
func (s SqlMeta) DeleteBurnObjectSegments(bucket, object string) error {
	if err := s.db.Where("bucket = ? AND object_name = ?", bucket, object).Delete(&burnbridgeObjectSegment{}).Error; err != nil {
		return mapSQLError("delete burn object segments", err)
	}
	return nil
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
	Bucket              string `json:"bucket"`
	VolumeLabel         string `json:"volumeLabel"`
	UpdatedAt           string `json:"updatedAt"` // RFC3339Nano
	TotalCapacityBytes  int64  `json:"totalCapacityBytes,omitempty"`
	FreeCapacityBytes   int64  `json:"freeCapacityBytes,omitempty"`
	MediaType           string `json:"mediaType,omitempty"`
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
	rows, err := s.db.Raw(
		`SELECT object_name, value FROM metadata_entries WHERE bucket = ? AND attribute = ? ORDER BY object_name`,
		bucket,
		BurnbridgeCommittedAttribute,
	).Rows()
	if err != nil {
		return nil, mapSQLError("list committed objects", err)
	}
	defer rows.Close()
	var out []CommittedObjectSummary
	for rows.Next() {
		var name sql.NullString
		var raw []byte
		if err := rows.Scan(&name, &raw); err != nil {
			return nil, mapSQLError("scan committed object", err)
		}
		if !name.Valid {
			continue
		}
		sum, err := committedSummaryFromJSON(name.String, raw)
		if err != nil {
			return nil, err
		}
		out = append(out, sum)
	}
	if err := rows.Err(); err != nil {
		return nil, mapSQLError("list committed objects", err)
	}
	return out, nil
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

		if err := tx.Model(&burnbridgeObjectSegment{}).
			Where("bucket = ? AND object_name = ?", bucket, oldObject).
			Update("object_name", newObject).Error; err != nil {
			return mapSQLError("rename burn segments", err)
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
