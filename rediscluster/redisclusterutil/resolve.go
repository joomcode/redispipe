package redisclusterutil

import "net"

// Resolve just resolves hostname:port to ipaddr:port and also returns original hostname
func Resolve(addr string) (string, string, error) {
	ip, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", "", err
	}
	ips, err := net.LookupHost(ip)
	if err != nil {
		return "", "", err
	}
	return net.JoinHostPort(ips[0], port), ip, nil
}

func IsIPAddress(addr string) bool {
	return net.ParseIP(addr) != nil
}
