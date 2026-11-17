package signing

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSignVerifyRoundTrip(t *testing.T) {
	secret := NewSecret()
	body := []byte(`{"id":"evt_1"}`)
	now := time.Unix(1_731_700_000, 0)
	h := Sign(secret, body, now)
	if err := Verify(secret, body, h, now.Add(time.Minute), 5*time.Minute); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

func TestVerifyRejects(t *testing.T) {
	secret := NewSecret()
	body := []byte("payload")
	now := time.Unix(1_731_700_000, 0)
	h := Sign(secret, body, now)

	cases := map[string]struct {
		secret, header string
		body           []byte
		at             time.Time
		want           error
	}{
		"tampered body":  {secret, h, []byte("payload!"), now, ErrMismatch},
		"wrong secret":   {NewSecret(), h, body, now, ErrMismatch},
		"replayed later": {secret, h, body, now.Add(10 * time.Minute), ErrExpired},
		"garbage":        {secret, "nonsense", body, now, ErrMalformed},
		"no v1":          {secret, "t=1731700000", body, now, ErrMalformed},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if err := Verify(c.secret, c.body, c.header, c.at, 5*time.Minute); !errors.Is(err, c.want) {
				t.Fatalf("got %v, want %v", err, c.want)
			}
		})
	}
}

func TestVerifyAcceptsAnyOfRotatedSecrets(t *testing.T) {
	oldS, newS := NewSecret(), NewSecret()
	body := []byte("x")
	now := time.Now()
	a := Sign(oldS, body, now)
	b := Sign(newS, body, now)
	// t=..,v1=old,v1=new
	combined := a + ",v1=" + strings.SplitN(b, "v1=", 2)[1]
	if err := Verify(newS, body, combined, now, time.Minute); err != nil {
		t.Fatal(err)
	}
}
