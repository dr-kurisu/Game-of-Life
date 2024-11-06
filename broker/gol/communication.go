package gol

type Request struct {
	World [][]byte
	Turn  int
	P     Params
}
type Response struct {
	World [][]byte
}

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

type PauseResponse struct {
	Turn int
}

type TickerResponse struct {
	AliveNum int
	Turn     int
}

type NoParams struct{}
