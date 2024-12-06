package embedstore

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/protomem/embedstore/pkg/rand"
)

func TestPager(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "test_data")

	t.Run("create pager", func(t *testing.T) {
		pgr, err := newPager(filename, os.Getpagesize())
		if err != nil {
			t.Fatalf(
				"Failed to create pager by path %s, with error %s",
				filename, err,
			)
		}
		defer pgr.close()

		for i := 0; i < 10; i++ {
			pg := pgr.createNext()
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

	t.Run("recovery pager", func(t *testing.T) {
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
			actualPgData := string(pg.data)

			if expectedPgData == actualPgData {
				t.Fatalf(
					"Failed to compare page data: expected %s, actual %s",
					expectedPgData, actualPgData,
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

	if expectedFlist.max != actualFlist.max {
		t.Fatalf(
			"Failed to compare freelist max page: expected %d, actual %d",
			expectedFlist.max, actualFlist.max,
		)
	}
	if len(expectedFlist.released) != len(actualFlist.released) {
		t.Fatalf(
			"Failed to compare freelist number released pages: expected %d, actual %d",
			len(expectedFlist.released), len(actualFlist.released),
		)
	}
}
