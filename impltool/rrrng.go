package impltool

func NextRng(state *uint32, mod uint32) uint32 {
	v := *state
	*state = v*0x12345 + 1
	return (v ^ v>>16) % mod
}
