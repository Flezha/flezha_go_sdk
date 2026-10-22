package flezha

import (
	"encoding/binary"
	"encoding/hex"
	"hash/fnv"
)

// stableID is the replay-stable identifier every SDK derives the same way:
// FNV-1a 128 over the cell identity, the change seq (big-endian u64) and the
// per-invocation ordinal (big-endian u32), as 32 lowercase hex characters.
func stableID(identity []byte, changeSeq uint64, ordinal uint32) string {
	h := fnv.New128a()
	h.Write(identity)
	var seq [8]byte
	binary.BigEndian.PutUint64(seq[:], changeSeq)
	h.Write(seq[:])
	var ord [4]byte
	binary.BigEndian.PutUint32(ord[:], ordinal)
	h.Write(ord[:])
	return hex.EncodeToString(h.Sum(nil))
}
