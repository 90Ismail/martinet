package kv

import (
	"bytes"
	"testing"
)

func TestPrefixEnd(t *testing.T) {
	cases := map[string][]byte{
		"att/":     []byte("att0"),
		"a\xff":    []byte("b"),
		"\xff\xff": nil,
	}
	for in, want := range cases {
		if got := PrefixEnd([]byte(in)); !bytes.Equal(got, want) {
			t.Errorf("PrefixEnd(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestAttemptKeysSortNumerically(t *testing.T) {
	// Zero padding keeps lexicographic order == attempt order in the LSM.
	if !(AttemptKey("d", 2) < AttemptKey("d", 10)) {
		t.Fatal("attempt keys must sort numerically")
	}
	if got := EventAttemptsPrefix("evt1"); got != "att/evt1:" {
		t.Fatal(got)
	}
	if k := AttemptKey(DeliveryID("evt1", "sub9"), 1); k[:len("att/evt1:")] != "att/evt1:" {
		t.Fatal("attempt key must share the event prefix")
	}
}
