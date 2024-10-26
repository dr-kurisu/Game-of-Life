package communication

import "uk.ac.bris.cs/gameoflife/gol"

type ReceiveFromServer struct {
	P            gol.Params
	CurrentWorld [][]byte
}
type SendToClient struct {
	World [][]byte
}
