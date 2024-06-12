package vt_store

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"io"
	"os"
	"strings"
)

type Store interface {
	Load() string
	Store(string) error
	Finish() error
}

type FileStore struct {
	file *os.File
	vt   string
}

type YdbStore struct {
	client    table.Client
	tableName string
	vt        string
}

func CreateFileStore(path string) (*FileStore, error) {
	fh, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	var fs FileStore
	if err != nil {
		return &fs, err
	}
	fs.file = fh

	b := new(strings.Builder)
	var sz int64
	sz, err = io.Copy(b, fs.file)
	if err != nil {
		return &fs, err
	}

	fs.vt = b.String()
	return &fs, nil
}

func (fs FileStore) Finish() error {
	return fs.file.Close()
}

func (fs FileStore) Load() string {
	return fs.vt
}

func (fs FileStore) Store(vt string) error {
	_, err := fs.file.Seek(0, 0)
	if err != nil {
		return err
	}

	var sz int
	sz, err = fs.file.WriteString(vt)

	if err != nil {
		return err
	}

	defer func() {
		if err == nil {
			err = fs.file.Sync()
			fs.vt = vt
		}
	}()

	if sz < len(fs.vt) {
		return fs.file.Truncate(int64(sz))
	}

	return nil
}
