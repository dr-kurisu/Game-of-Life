package gol

import "net/rpc"

type RequestBroker struct {
	World   [][]byte
	Clients []*rpc.Client
	Turn    int
	P       Params
}

type ResponseBroker struct {
	World [][]byte
}
