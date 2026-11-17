// Package signing implements webhook signatures.
//
// Header format (one header, comma separated):
//
//	Martinet-Signature: t=1731700000,v1=5257a869e7ecebeda32affa62cdca3fa51cad7e77a0e56ff536d0ce8e108d8bd
//
// v1 = hex(HMAC-SHA256(secret, "<t>.<body>")). Including the timestamp in
// the MAC lets receivers reject replays older than a tolerance window.
package signing

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Header is the HTTP header carrying the signature.
const Header = "Martinet-Signature"

var (
	ErrMalformed = errors.New("signing: malformed signature header")
	ErrMismatch  = errors.New("signing: signature mismatch")
	ErrExpired   = errors.New("signing: timestamp outside tolerance")
)

// NewSecret returns a random signing secret ("whsec_" + 32 bytes hex).
func NewSecret() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return "whsec_" + hex.EncodeToString(b)
}

func mac(secret string, ts int64, body []byte) string {
	h := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(h, "%d.", ts)
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

// Sign returns the header value for body at time now.
func Sign(secret string, body []byte, now time.Time) string {
	ts := now.Unix()
	return fmt.Sprintf("t=%d,v1=%s", ts, mac(secret, ts, body))
}

// Verify checks a header value. Multiple v1 entries are allowed so senders
// can roll secrets without downtime.
func Verify(secret string, body []byte, header string, now time.Time, tolerance time.Duration) error {
	var ts int64
	var sigs []string
	for _, part := range strings.Split(header, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return ErrMalformed
		}
		switch k {
		case "t":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return ErrMalformed
			}
			ts = n
		case "v1":
			sigs = append(sigs, v)
		}
	}
	if ts == 0 || len(sigs) == 0 {
		return ErrMalformed
	}
	if d := now.Sub(time.Unix(ts, 0)); d > tolerance || d < -tolerance {
		return ErrExpired
	}
	want := mac(secret, ts, body)
	for _, s := range sigs {
		if hmac.Equal([]byte(s), []byte(want)) { // constant time
			return nil
		}
	}
	return ErrMismatch
}
