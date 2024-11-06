package gol

type RequestBroker struct {
	World [][]byte
	Ports []string
	Turn  int
	P     Params
}

type ResponseBroker struct {
	World      [][]byte
	FinalTurns int
}

type TickerResponse struct {
	AliveNum int
	Turn     int
}

type PauseResponse struct {
	Turn int
}

type NoParams struct{}
