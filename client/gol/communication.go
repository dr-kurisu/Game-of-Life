package gol

type RequestBroker struct {
	World [][]byte
	Ports []string
	Turn  int
	P     Params
}

type ResponseBroker struct {
	World [][]byte
}
