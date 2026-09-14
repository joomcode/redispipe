package rediscluster

import (
	"reflect"
	"testing"
)

func TestHostMasks(t *testing.T) {
	for _, tc := range []struct {
		name   string
		health uint32
		all    uint32
		want   []uint32
	}{
		{name: "all healthy", health: 0b11, all: 0b11, want: []uint32{0b11}},
		{name: "replica unhealthy", health: 0b01, all: 0b11, want: []uint32{0b01, 0b11}},
		{name: "nobody healthy", health: 0, all: 0b111, want: []uint32{0, 0b111}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			masks, n := hostMasks(tc.health, tc.all)
			if got := masks[:n]; !reflect.DeepEqual(got, tc.want) {
				t.Errorf("hostMasks(%b, %b) = %b, want %b", tc.health, tc.all, got, tc.want)
			}
		})
	}
}

func TestHostWalk(t *testing.T) {
	for _, tc := range []struct {
		name    string
		weights []uint32
		mask    uint32
		want    []uint
	}{
		{name: "masked out host is skipped", weights: []uint32{1, 1}, mask: 0b01, want: []uint{0}},
		{name: "every host of the mask is yielded", weights: []uint32{1, 1}, mask: 0b11, want: []uint{0, 1}},
		{name: "zero weights still make progress", weights: []uint32{0, 0, 0}, mask: 0b111, want: []uint{0, 1, 2}},
		{name: "zero weight of a masked out host", weights: []uint32{0, 1}, mask: 0b11, want: []uint{1, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			off := uint32(0)
			walk := newHostWalk(tc.weights, tc.mask, &off)
			var got []uint
			for {
				k, ok := walk.next()
				if !ok {
					break
				}
				got = append(got, k)
			}
			if !reflect.DeepEqual(sorted(got), sorted(tc.want)) {
				t.Errorf("walk over %b yielded %v, want %v", tc.mask, got, tc.want)
			}
		})
	}
}

func sorted(ks []uint) []uint {
	out := append([]uint(nil), ks...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}
