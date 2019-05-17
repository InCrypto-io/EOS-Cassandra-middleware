package storage

import (
	"fmt"
	"testing"
)


func Test_GetActionsArgsNormalization(t *testing.T) {
	create_int64 := func(i int64) *int64 { return &i }
	type NormalizedGetActionArgs struct {
		Pos int64
		Count int64
		Order bool
	}
	type Test struct {
		Args GetActionArgs
		Normalized NormalizedGetActionArgs
	}
	tests := []Test{
		{ Args: GetActionArgs{ Pos: create_int64(-55), Offset: create_int64(-73) },
			Normalized: NormalizedGetActionArgs{ Pos: 0, Count: 0, Order: true } },
		{ Args: GetActionArgs{ Pos: create_int64(-9), Offset: create_int64(-1) },
			Normalized: NormalizedGetActionArgs{ Pos: 0, Count: 0, Order: true } },
		{ Args: GetActionArgs{ Pos: create_int64(3), Offset: create_int64(9) },
			Normalized: NormalizedGetActionArgs{ Pos: 3, Count: 10, Order: true } },
		{ Args: GetActionArgs{ Pos: create_int64(80), Offset: create_int64(87) },
			Normalized: NormalizedGetActionArgs{ Pos: 80, Count: 88, Order: true } },
		{ Args: GetActionArgs{ Pos: create_int64(6), Offset: create_int64(-9) },
			Normalized: NormalizedGetActionArgs{ Pos: 0, Count: 7, Order: true } },
		{ Args: GetActionArgs{ Pos: create_int64(0), Offset: create_int64(0) },
			Normalized: NormalizedGetActionArgs{ Pos: 0, Count: 1, Order: true } },
		{ Args: GetActionArgs{ Pos: create_int64(-1), Offset: create_int64(-8) },
			Normalized: NormalizedGetActionArgs{ Pos: 0, Count: 8, Order: false } },
		{ Args: GetActionArgs{ Pos: create_int64(-1), Offset: create_int64(7) },
			Normalized: NormalizedGetActionArgs{ Pos: 0, Count: 0, Order: false } },
		{ Args: GetActionArgs{ Pos: create_int64(-2), Offset: create_int64(3) },
			Normalized: NormalizedGetActionArgs{ Pos: 0, Count: 2, Order: true } },
	}
	for _, test := range tests {
		pos, count, order := test.Args.Normalize()
		if pos != test.Normalized.Pos ||
			count != test.Normalized.Count ||
			order != test.Normalized.Order {
			t.Error(fmt.Sprintf("Expected: pos=%d; count=%d; order=%t\nGot: pos=%d; count=%d; order=%t", test.Normalized.Pos, test.Normalized.Count, test.Normalized.Order, pos, count, order))
		}
	}
}