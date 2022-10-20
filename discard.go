/*
 * Copyright 2020 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/dgraph-io/badger/v3/y"
	"github.com/dgraph-io/ristretto/z"
)

// discardStats keeps track of the amount of data that could be discarded for
// a given logfile.
type discardStats struct {
	sync.Mutex

	*z.MmapFile
	opt           Options
	nextEmptySlot int
}

const discardFname string = "DISCARD"

func InitDiscardStats(opt Options) (*discardStats, error) {
	fname := filepath.Join(opt.ValueDir, discardFname)//目录下面的DISCARD文件

	// 1GB file can store 67M discard entries. Each entry is 16 bytes.
	mf, err := z.OpenMmapFile(fname, os.O_CREATE|os.O_RDWR, 1<<20)
	lf := &discardStats{
		MmapFile: mf,
		opt:      opt,
	}
	if err == z.NewFile {
		// We don't need to zero out the entire 1GB.
		lf.zeroOut()

	} else if err != nil {
		return nil, y.Wrapf(err, "while opening file: %s\n", discardFname)
	}

	for slot := 0; slot < lf.maxSlot(); slot++ {
		if lf.get(16*slot) == 0 {
			lf.nextEmptySlot = slot
			break
		}
	}
	sort.Sort(lf)
	opt.Infof("Discard stats nextEmptySlot: %d\n", lf.nextEmptySlot)
	return lf, nil
}

func (lf *discardStats) Len() int {
	return lf.nextEmptySlot
}
func (lf *discardStats) Less(i, j int) bool {
	return lf.get(16*i) < lf.get(16*j)
}
func (lf *discardStats) Swap(i, j int) {
	left := lf.Data[16*i : 16*i+16]
	right := lf.Data[16*j : 16*j+16]
	var tmp [16]byte
	copy(tmp[:], left)
	copy(left, right)
	copy(right, tmp[:])
}

// offset is not slot.
func (lf *discardStats) get(offset int) uint64 {//对discard映射出来的data读，就是对文件进行读
	return binary.BigEndian.Uint64(lf.Data[offset : offset+8])
}
func (lf *discardStats) set(offset int, val uint64) {//对discard映射出来的data写，就是对文件进行写
	binary.BigEndian.PutUint64(lf.Data[offset:offset+8], val)
}

// zeroOut would zero out the next slot.
func (lf *discardStats) zeroOut() {//新创建的discard文件的nextEmptySlot=0，对第一个槽位（slot）写0
	lf.set(lf.nextEmptySlot*16, 0)//第0-7byte 写0
	lf.set(lf.nextEmptySlot*16+8, 0)//第8-15byte 写0
}

func (lf *discardStats) maxSlot() int {
	return len(lf.Data) / 16//每个slot的大小是16bytes
}

// Update would update the discard stats for the given file id. If discard is
// 0, it would return the current value of discard for the file. If discard is
// < 0, it would set the current value of discard to zero for the file.
func (lf *discardStats) Update(fidu uint32, discard int64) int64 {
	fid := uint64(fidu)
	lf.Lock()
	defer lf.Unlock()

	idx := sort.Search(lf.nextEmptySlot, func(slot int) bool {
		return lf.get(slot*16) >= fid
	})
	if idx < lf.nextEmptySlot && lf.get(idx*16) == fid {
		off := idx*16 + 8
		curDisc := lf.get(off)
		if discard == 0 {//discard = 0 就直接返回当前的discard
			return int64(curDisc)
		}
		if discard < 0 {//discard < 0 就直接返回当前的discard为0，并返回0
			lf.set(off, 0)
			return 0
		}
		lf.set(off, curDisc+uint64(discard))//discard > 0 时，写入原来存储的discard+当前传入discard，并返回
		return int64(curDisc + uint64(discard))
	}
	if discard <= 0 {
		// No need to add a new entry.
		return 0
	}

	// Could not find the fid. Add the entry.
	idx = lf.nextEmptySlot//没找到，则新加一项，并且存入当前的discard，返回值值也是当前存入的discard
	lf.set(idx*16, uint64(fid))
	lf.set(idx*16+8, uint64(discard))

	// Move to next slot.
	lf.nextEmptySlot++
	for lf.nextEmptySlot >= lf.maxSlot() {//容量不够存储slot的时候，直接扩容到两倍
		y.Check(lf.Truncate(2 * int64(len(lf.Data))))
	}
	lf.zeroOut()//当前的Slot的16字节置0

	sort.Sort(lf)//按slot的前8个字节也就是fid来排序
	return int64(discard)
}

func (lf *discardStats) Iterate(f func(fid, stats uint64)) {
	for slot := 0; slot < lf.nextEmptySlot; slot++ {
		idx := 16 * slot
		f(lf.get(idx), lf.get(idx+8))
	}
}

// MaxDiscard returns the file id with maximum discard bytes.
func (lf *discardStats) MaxDiscard() (uint32, int64) {//找出最大的discard对应的fid
	lf.Lock()
	defer lf.Unlock()

	var maxFid, maxVal uint64
	lf.Iterate(func(fid, val uint64) {
		if maxVal < val {
			maxVal = val
			maxFid = fid
		}
	})
	return uint32(maxFid), int64(maxVal)
}
