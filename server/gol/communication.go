package gol

type Request struct {
	World [][]byte
	Turn  int
	P     Params
}

type Response struct {
	World [][]byte
}

type NoParams struct{}
