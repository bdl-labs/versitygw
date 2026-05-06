package meta

import (
	"errors"
	"testing"

	"github.com/versity/versitygw/s3err"
)

func TestMapSQLError_NoSpace(t *testing.T) {
	err := mapSQLError("store", errors.New("database or disk is full"))
	apiErr := s3err.GetAPIError(s3err.ErrNoSpaceLeftOnDevice)
	if err == nil || err.Error() != apiErr.Error() {
		t.Fatalf("expected %v, got %v", apiErr, err)
	}
}

func TestMapSQLError_ReadOnly(t *testing.T) {
	err := mapSQLError("store", errors.New("attempt to write a readonly database"))
	apiErr := s3err.GetAPIError(s3err.ErrMethodNotAllowed)
	if err == nil || err.Error() != apiErr.Error() {
		t.Fatalf("expected %v, got %v", apiErr, err)
	}
}

func TestMapSQLError_GenericWrap(t *testing.T) {
	err := mapSQLError("store", errors.New("some failure"))
	if err == nil {
		t.Fatal("expected non-nil error")
	}
	if got := err.Error(); got == "some failure" {
		t.Fatalf("expected wrapped error, got %q", got)
	}
}
