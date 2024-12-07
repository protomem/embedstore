package embedstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

const (
	_defaultFilePerm = 0o666

	_defaultMetaPage  pagenum = 0
	_defaultFlistPage pagenum = _defaultMetaPage + 1

	_beginFreeBlocks pagenum = _defaultFlistPage + 1
)

var ErrWrongBytes = errors.New("wrong number of bytes")

type pagenum int64

type page struct {
	num  pagenum
	data []byte
}

func newPage(num pagenum, size int) *page {
	return &page{
		num:  num,
		data: make([]byte, size),
	}
}

func (pg *page) withNum(num pagenum) *page {
	pg.num = num
	return &page{
		num:  num,
		data: pg.data,
	}
}

func (pg *page) write(b []byte) {
	copy(pg.data, b)
}

type pager struct {
	path string
	f    *os.File

	psize int

	meta  *metainfo
	flist *freelist
}

func newPager(path string, psize int) (*pager, error) {
	exists, err := isFsEntryExists(path)
	if err != nil {
		return nil, fmt.Errorf("pager/new: %w", err)
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, _defaultFilePerm)
	if err != nil {
		return nil, fmt.Errorf("pager/new: open/create file: %w", err)
	}

	pgr := &pager{
		path: path,
		f:    f,

		psize: psize,

		meta:  newMetainfo(),
		flist: newFreelist(),
	}

	if exists {
		if err := pgr.recovery(); err != nil {
			_ = pgr.close()
			return nil, fmt.Errorf("pager/new: %w", err)
		}
	} else {
		if err := pgr.flush(); err != nil {
			_ = pgr.close()
			return nil, fmt.Errorf("pager/new: %w", err)
		}
	}

	return pgr, nil
}

func isFsEntryExists(path string) (bool, error) {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (pgr *pager) alloc() *page {
	return newPage(0, pgr.psize)
}

func (pgr *pager) write(pg *page) error {
	off := int64(pg.num) * int64(pgr.psize)

	if _, err := pgr.f.WriteAt(pg.data, off); err != nil {
		return fmt.Errorf(
			"pager/write(num=%d,size=%d): %w",
			pg.num, len(pg.data), err,
		)
	}

	return nil
}

func (pgr *pager) read(num pagenum) (*page, error) {
	pg := pgr.alloc().withNum(num)
	off := int64(num) * int64(pgr.psize)

	if _, err := pgr.f.ReadAt(pg.data, off); err != nil {
		return nil, fmt.Errorf("pager/read(num=%d): %w", pg.num, err)
	}

	return pg, nil
}

func (pgr *pager) flush() error {
	metapg := pgr.alloc().withNum(_defaultMetaPage)
	metab := pgr.meta.serialize()

	copy(metapg.data, metab)

	if err := pgr.write(metapg); err != nil {
		return fmt.Errorf("pager: flush metainfo: %w", err)
	}

	flistpg := pgr.alloc().withNum(pgr.meta.flist)
	flistb := pgr.flist.serialize()

	copy(flistpg.data, flistb)

	if err := pgr.write(flistpg); err != nil {
		return fmt.Errorf("pager: flush freelist: %w", err)
	}

	return nil
}

func (pgr *pager) recovery() error {
	metapg, err := pgr.read(_defaultMetaPage)
	if err != nil {
		return fmt.Errorf("pager: recover metainfo: %w", err)
	}

	if err := pgr.meta.deserialize(metapg.data); err != nil {
		return fmt.Errorf("pager: recover metainfo: %w", err)
	}

	flistpg, err := pgr.read(pgr.meta.flist)
	if err != nil {
		return fmt.Errorf("pager: recover freelist: %w", err)
	}

	if err := pgr.flist.deserialize(flistpg.data); err != nil {
		return fmt.Errorf("pager: recover freelist: %w", err)
	}

	return nil
}

func (pgr *pager) close() error {
	if err := pgr.f.Close(); err != nil {
		return fmt.Errorf("pager/close: %w", err)
	}

	return nil
}

type metainfo struct {
	flist pagenum
}

func newMetainfo() *metainfo {
	return &metainfo{
		flist: _defaultFlistPage,
	}
}

func (meta *metainfo) serialize() []byte {
	b := make([]byte, 8)

	binary.LittleEndian.PutUint64(b[:8], uint64(meta.flist))

	return b
}

func (meta *metainfo) deserialize(b []byte) error {
	if len(b) < 8 {
		return fmt.Errorf("meta/deserialize: %w", ErrWrongBytes)
	}

	meta.flist = pagenum(binary.LittleEndian.Uint64(b[:8]))

	return nil
}

type freelist struct {
	max      pagenum
	released []pagenum
}

func newFreelist() *freelist {
	return &freelist{
		max:      _beginFreeBlocks,
		released: make([]pagenum, 0),
	}
}

func (flist *freelist) next() pagenum {
	if len(flist.released) == 0 {
		curr := flist.max
		flist.max += 1
		return curr
	}

	num := flist.released[len(flist.released)-1]
	flist.released = flist.released[:len(flist.released)-1]

	return num
}

func (flist *freelist) release(num pagenum) {
	if num <= _beginFreeBlocks {
		return
	}

	flist.released = append(flist.released, num)
}

func (flist *freelist) serialize() []byte {
	b := make([]byte, 8+4+(8*len(flist.released)))

	binary.LittleEndian.PutUint64(b[:8], uint64(flist.max))
	binary.LittleEndian.PutUint32(b[8:12], uint32(len(flist.released)))

	for i, num := range flist.released {
		binary.LittleEndian.PutUint64(b[12+(8*i):(12+(8*i))+8], uint64(num))
	}

	return b
}

func (flist *freelist) deserialize(b []byte) error {
	if len(b) < 8+4 {
		return fmt.Errorf("freelist/deserialize: decode head: %w", ErrWrongBytes)
	}

	flist.max = pagenum(binary.LittleEndian.Uint64(b[:8]))
	flist.released = make([]pagenum, binary.LittleEndian.Uint32(b[8:12]))

	if len(b) < (8+4)+(8*len(flist.released)) {
		return fmt.Errorf("freelist/deserialize: decode body: %w", ErrWrongBytes)
	}

	for i := range flist.released {
		flist.released[i] = pagenum(binary.LittleEndian.Uint64(b[12+(8*i) : (12+(8*i))+8]))
	}

	return nil
}
