// Package ids generates time-ordered, globally unique identifiers.
//
// Format: <prefix>_<13 hex ms timestamp><16 hex random>. Lexicographic order
// matches creation order (to the millisecond), which keeps keys that embed
// ids clustered by time in the LSM tree.
package ids

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// New returns e.g. "evt_0193a1b2c3d4e9f1c2a3b4c5d6e7f8a9".
func New(prefix string) string {
	var r [8]byte
	if _, err := rand.Read(r[:]); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s_%013x%s", prefix, time.Now().UnixMilli(), hex.EncodeToString(r[:]))
}
