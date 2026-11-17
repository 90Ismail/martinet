package main

import "testing"

func TestMatches(t *testing.T) {
	cases := []struct {
		patterns []string
		typ      string
		want     bool
	}{
		{nil, "order.created", true},
		{[]string{"*"}, "anything", true},
		{[]string{"order.created"}, "order.created", true},
		{[]string{"order.created"}, "order.updated", false},
		{[]string{"order.*"}, "order.updated", true},
		{[]string{"order.*"}, "orders.updated", false},
		{[]string{"invoice.paid", "order.*"}, "order.shipped", true},
	}
	for _, c := range cases {
		if got := Matches(c.patterns, c.typ); got != c.want {
			t.Errorf("Matches(%v, %q) = %v, want %v", c.patterns, c.typ, got, c.want)
		}
	}
}
