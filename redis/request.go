package redis

type Request struct {
	Cmd  string
	Args []interface{}
}
