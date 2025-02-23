package data_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/protomem/embedstore/data"
	"github.com/protomem/embedstore/pkg/rand"
)

func TestPager(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "test_data")

	t.Run("create and write", func(t *testing.T) {
		pgr, err := data.NewPager(filename, os.Getpagesize())
		if err != nil {
			t.Fatalf(
				"Failed to create pager by path %s, with error %s",
				filename, err,
			)
		}
		defer pgr.Close()

		for i := 0; i < 10; i++ {
			pg := pgr.Alloc().WithNum(pgr.Freelist().Next())
			pg.Write([]byte(fmt.Sprintf("data%d", i+1)))

			if err := pgr.Write(pg); err != nil {
				t.Fatalf(
					"Failed to write page %+v, with error %s",
					pg, err,
				)
			}
		}

		if err := pgr.Flush(); err != nil {
			t.Fatalf("Failed to flush metainfo, with error %s", err)
		}
	})

	t.Run("recovery and read", func(t *testing.T) {
		pgr, err := data.NewPager(filename, os.Getpagesize())
		if err != nil {
			t.Fatalf(
				"Failed to open pager by path %s, with error %s",
				filename, err,
			)
		}
		defer pgr.Close()

		for i, p := 0, data.BeginFreeBlocks; i < 10; i, p = i+1, p+1 {
			pg, err := pgr.Read(data.PageNum(p))
			if err != nil {
				t.Fatalf(
					"Failed to read page %d, with error %s",
					p, err,
				)
			}

			expectedPgData := fmt.Sprintf("data%d", i+1)
			actualPgData := string(bytes.TrimRight(pg.Data, "\x00"))

			if expectedPgData != actualPgData {
				t.Fatalf(
					"Failed to compare page data: expected %s(%d), actual %s(%d)",
					expectedPgData, len(expectedPgData),
					actualPgData, len(actualPgData),
				)
			}
		}
	})
}

func TestMetainfo_Serialization(t *testing.T) {
	expectedMeta := data.NewMetainfo()
	expectedMeta.Freelist = data.PageNum(rand.Range(1, 100))
	expectedMetab := expectedMeta.Serialize()

	actualMeta := new(data.Metainfo)
	if err := actualMeta.Deserialize(expectedMetab); err != nil {
		t.Fatalf(
			"Failed to deserialize metainfo %+v, with error %s",
			expectedMeta, err,
		)
	}

	if !expectedMeta.Equal(actualMeta) {
		t.Fatalf(
			"Failed to compare metainfo: expected %+v, actual %+v",
			expectedMeta, actualMeta,
		)
	}
}

func TestFreelist_Serialization(t *testing.T) {
	expectedFlist := data.NewFreelist()
	for i := 0; i < 10; i++ {
		expectedFlist.Next()
	}
	for i := 0; i < 10; i++ {
		if rand.Bool() {
			expectedFlist.Release(data.PageNum(i))
		}
	}
	expectedFlistb := expectedFlist.Serialize()

	actualFlist := new(data.Freelist)
	if err := actualFlist.Deserialize(expectedFlistb); err != nil {
		t.Fatalf(
			"Failed to deserialize freelist %+v, with error %s",
			expectedFlist, err,
		)
	}

	if !expectedFlist.Equal(actualFlist) {
		t.Fatalf(
			"Failed to check for equals freelists: expected %d, actual %d",
			expectedFlist, actualFlist,
		)
	}
}
