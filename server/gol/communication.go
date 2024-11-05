package gol

type Request struct {
	// world with two extra line
	World [][]byte
	Turn  int
	P     Params
}
type Response struct {
	World [][]byte
}
