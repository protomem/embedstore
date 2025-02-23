package data

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

const (
	DefaultFilePerm = 0o666

	DefaultMetaPage  PageNum = 0
	DefaultFlistPage PageNum = DefaultMetaPage + 1

	BeginFreeBlocks PageNum = DefaultFlistPage + 1
)

var ErrWrongBytes = errors.New("wrong number of bytes")

type PageNum int64

type Page struct {
	Num  PageNum
	Data []byte
}

func NewPage(num PageNum, size int) *Page {
	return &Page{
		Num:  num,
		Data: make([]byte, size),
	}
}

func (pg *Page) WithNum(num PageNum) *Page {
	pg.Num = num
	return &Page{
		Num:  num,
		Data: pg.Data,
	}
}

func (pg *Page) Write(b []byte) {
	copy(pg.Data, b)
}

type Pager struct {
	path string
	f    *os.File

	psize int

	meta  *Metainfo
	flist *Freelist
}

func NewPager(path string, psize int) (*Pager, error) {
	var err error

	exists, err := isFsEntryExists(path)
	if err != nil {
		return nil, fmt.Errorf("pager/new: %w", err)
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, DefaultFilePerm)
	if err != nil {
		return nil, fmt.Errorf("pager/new: open/create file: %w", err)
	}

	pgr := &Pager{
		path: path,
		f:    f,

		psize: psize,

		meta:  NewMetainfo(),
		flist: NewFreelist(),
	}

	if exists {
		err = pgr.Recovery()
	} else {
		err = pgr.Flush()
	}

	if err != nil {
		_ = pgr.Close()
		return nil, fmt.Errorf("pager/new: %w", err)
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

func (pgr *Pager) Alloc() *Page {
	return NewPage(0, pgr.psize)
}

func (pgr *Pager) Write(pg *Page) error {
	off := int64(pg.Num) * int64(pgr.psize)

	if _, err := pgr.f.WriteAt(pg.Data, off); err != nil {
		return fmt.Errorf(
			"pager/write(num=%d,size=%d): %w",
			pg.Num, len(pg.Data), err,
		)
	}

	return nil
}

func (pgr *Pager) Read(num PageNum) (*Page, error) {
	pg := pgr.Alloc().WithNum(num)
	off := int64(num) * int64(pgr.psize)

	if _, err := pgr.f.ReadAt(pg.Data, off); err != nil {
		return nil, fmt.Errorf("pager/read(num=%d): %w", pg.Num, err)
	}

	return pg, nil
}

func (pgr *Pager) Flush() error {
	metapg := pgr.Alloc().WithNum(DefaultMetaPage)
	metab := pgr.meta.Serialize()

	copy(metapg.Data, metab)

	if err := pgr.Write(metapg); err != nil {
		return fmt.Errorf("pager: flush metainfo: %w", err)
	}

	flistpg := pgr.Alloc().WithNum(pgr.meta.Freelist)
	flistb := pgr.flist.Serialize()

	copy(flistpg.Data, flistb)

	if err := pgr.Write(flistpg); err != nil {
		return fmt.Errorf("pager: flush freelist: %w", err)
	}

	return nil
}

func (pgr *Pager) Recovery() error {
	metapg, err := pgr.Read(DefaultMetaPage)
	if err != nil {
		return fmt.Errorf("pager: recover metainfo: %w", err)
	}

	if err := pgr.meta.Deserialize(metapg.Data); err != nil {
		return fmt.Errorf("pager: recover metainfo: %w", err)
	}

	flistpg, err := pgr.Read(pgr.meta.Freelist)
	if err != nil {
		return fmt.Errorf("pager: recover freelist: %w", err)
	}

	if err := pgr.flist.Deserialize(flistpg.Data); err != nil {
		return fmt.Errorf("pager: recover freelist: %w", err)
	}

	return nil
}

func (pgr *Pager) Close() error {
	if err := pgr.f.Close(); err != nil {
		return fmt.Errorf("pager/close: %w", err)
	}

	return nil
}

func (pgr *Pager) Meta() *Metainfo {
	return pgr.meta
}

func (pgr *Pager) Freelist() *Freelist {
	return pgr.flist
}

type Metainfo struct {
	Freelist PageNum
}

func NewMetainfo() *Metainfo {
	return &Metainfo{
		Freelist: DefaultFlistPage,
	}
}

func (meta *Metainfo) Serialize() []byte {
	b := make([]byte, 8)

	binary.LittleEndian.PutUint64(b[:8], uint64(meta.Freelist))

	return b
}

func (meta *Metainfo) Deserialize(b []byte) error {
	if len(b) < 8 {
		return fmt.Errorf("meta/deserialize: %w", ErrWrongBytes)
	}

	meta.Freelist = PageNum(binary.LittleEndian.Uint64(b[:8]))

	return nil
}

func (meta *Metainfo) Equal(other *Metainfo) bool {
	return meta.Freelist == other.Freelist
}

type Freelist struct {
	Max      PageNum
	Released []PageNum
}

func NewFreelist() *Freelist {
	return &Freelist{
		Max:      BeginFreeBlocks,
		Released: make([]PageNum, 0),
	}
}

func (flist *Freelist) Next() PageNum {
	if len(flist.Released) == 0 {
		curr := flist.Max
		flist.Max += 1
		return curr
	}

	num := flist.Released[len(flist.Released)-1]
	flist.Released = flist.Released[:len(flist.Released)-1]

	return num
}

func (flist *Freelist) Release(num PageNum) {
	if num <= BeginFreeBlocks {
		return
	}

	flist.Released = append(flist.Released, num)
}

func (flist *Freelist) Serialize() []byte {
	b := make([]byte, 8+4+(8*len(flist.Released)))

	binary.LittleEndian.PutUint64(b[:8], uint64(flist.Max))
	binary.LittleEndian.PutUint32(b[8:12], uint32(len(flist.Released)))

	for i, num := range flist.Released {
		binary.LittleEndian.PutUint64(b[12+(8*i):(12+(8*i))+8], uint64(num))
	}

	return b
}

func (flist *Freelist) Deserialize(b []byte) error {
	if len(b) < 8+4 {
		return fmt.Errorf("freelist/deserialize: decode head: %w", ErrWrongBytes)
	}

	flist.Max = PageNum(binary.LittleEndian.Uint64(b[:8]))
	flist.Released = make([]PageNum, binary.LittleEndian.Uint32(b[8:12]))

	if len(b) < (8+4)+(8*len(flist.Released)) {
		return fmt.Errorf("freelist/deserialize: decode body: %w", ErrWrongBytes)
	}

	for i := range flist.Released {
		flist.Released[i] = PageNum(binary.LittleEndian.Uint64(b[12+(8*i) : (12+(8*i))+8]))
	}

	return nil
}

func (flist *Freelist) Equal(other *Freelist) bool {
	if flist.Max != other.Max || len(flist.Released) != len(other.Released) {
		return false
	}

	for i := range flist.Released {
		exists := false
		for j := range other.Released {
			if flist.Released[i] == other.Released[j] {
				exists = true
				break
			}
		}
		if !exists {
			return false
		}
	}

	return true
}
