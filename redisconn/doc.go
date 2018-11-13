/*
Package redisconn implements connection to single redis server.

Connection is "wrapper" around single tcp (unix-socket) connection. All requests are fed into
single connection, and responses are asynchronously read from.
Connection is thread-safe, ie it doesn't need external synchronization.
Connect is responsible for reconnection, but it does no requests retrying in case of networking problems.
*/
package redisconn
