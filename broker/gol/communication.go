package gol

import "uk.ac.bris.cs/gameoflife/util"

type Request struct {
	World [][]byte
	Turn  int
	P     Params
}
type Response struct {
	World [][]byte
}

type RequestBroker struct {
	Cells []util.Cell
	Ports []string
	Turn  int
	P     Params
}

type ResponseBroker struct {
	Cells      []util.Cell
	FinalTurns int
}

type PauseResponse struct {
	Turn int
}

type CellFlipResponse struct {
	Cells []util.Cell
	Turn  int
}

type TickerResponse struct {
	AliveNum int
	Turn     int
}

type NoParams struct{}
