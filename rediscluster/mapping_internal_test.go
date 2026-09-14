package rediscluster

import "testing"

func TestPickHost(t *testing.T) {
	for _, tc := range []struct {
		name    string
		weights []uint32
		mask    uint32
		off     uint32
		want    uint
	}{
		{name: "masked out host is skipped", weights: []uint32{1, 1}, mask: 0b10, off: 0, want: 1},
		{name: "wheel slot 0 belongs to the light host", weights: []uint32{1, 1000}, mask: 0b11, off: 0, want: 0},
		{name: "wheel slot 5 belongs to the heavy host", weights: []uint32{1, 1000}, mask: 0b11, off: 5, want: 1},
		{name: "zero weights pick the lowest host", weights: []uint32{0, 0, 0}, mask: 0b110, off: 0, want: 1},
		{name: "zero weight host is never picked while others remain", weights: []uint32{0, 1}, mask: 0b11, off: 0, want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			total := uint32(0)
			for i, w := range tc.weights {
				if tc.mask&(1<<uint(i)) != 0 {
					total += w
				}
			}
			off := tc.off
			if got := pickHost(tc.weights, tc.mask, total, &off); got != tc.want {
				t.Errorf("pickHost(%v, %b, %d, %d) = %d, want %d", tc.weights, tc.mask, total, tc.off, got, tc.want)
			}
		})
	}
}
