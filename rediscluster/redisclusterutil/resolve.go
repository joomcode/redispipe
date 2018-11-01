package redisclusterutil

import "net"

// Resolve just resolves hostname:port to ipaddr:port
func Resolve(addr string) (string, error) {
	ip, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	ips, err := net.LookupHost(ip)
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(ips[0], port), nil
}
