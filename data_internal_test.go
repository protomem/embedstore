package embedstore

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/protomem/embedstore/pkg/rand"
)

func TestPager(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "test_data")

	t.Run("create and write", func(t *testing.T) {
		pgr, err := newPager(filename, os.Getpagesize())
		if err != nil {
			t.Fatalf(
				"Failed to create pager by path %s, with error %s",
				filename, err,
			)
		}
		defer pgr.close()

		for i := 0; i < 10; i++ {
			pg := pgr.alloc().withNum(pgr.flist.next())
			pg.write([]byte(fmt.Sprintf("data%d", i+1)))

			if err := pgr.write(pg); err != nil {
				t.Fatalf(
					"Failed to write page %+v, with error %s",
					pg, err,
				)
			}
		}

		if err := pgr.flush(); err != nil {
			t.Fatalf("Failed to flush metainfo, with error %s", err)
		}
	})

	t.Run("recovery and read", func(t *testing.T) {
		pgr, err := newPager(filename, os.Getpagesize())
		if err != nil {
			t.Fatalf(
				"Failed to open pager by path %s, with error %s",
				filename, err,
			)
		}
		defer pgr.close()

		for i, p := 0, _beginFreeBlocks; i < 10; i, p = i+1, p+1 {
			pg, err := pgr.read(pagenum(p))
			if err != nil {
				t.Fatalf(
					"Failed to read page %d, with error %s",
					p, err,
				)
			}

			expectedPgData := fmt.Sprintf("data%d", i+1)
			actualPgData := string(bytes.TrimRight(pg.data, "\x00"))

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
	expectedMeta := newMetainfo()
	expectedMeta.flist = pagenum(rand.Range(1, 100))
	expectedMetab := expectedMeta.serialize()

	actualMeta := new(metainfo)
	if err := actualMeta.deserialize(expectedMetab); err != nil {
		t.Fatalf(
			"Failed to deserialize metainfo %+v, with error %s",
			expectedMeta, err,
		)
	}

	if *expectedMeta != *actualMeta {
		t.Fatalf(
			"Failed to compare metainfo: expected %+v, actual %+v",
			expectedMeta, actualMeta,
		)
	}
}

func TestFreelist_Serialization(t *testing.T) {
	expectedFlist := newFreelist()
	for i := 0; i < 10; i++ {
		expectedFlist.next()
	}
	for i := 0; i < 10; i++ {
		if rand.Bool() {
			expectedFlist.release(pagenum(i))
		}
	}
	expectedFlistb := expectedFlist.serialize()

	actualFlist := new(freelist)
	if err := actualFlist.deserialize(expectedFlistb); err != nil {
		t.Fatalf(
			"Failed to deserialize freelist %+v, with error %s",
			expectedFlist, err,
		)
	}

	if !expectedFlist.isEqual(actualFlist) {
		t.Fatalf(
			"Failed to check for equals freelists: expected %d, actual %d",
			expectedFlist, actualFlist,
		)
	}
}
