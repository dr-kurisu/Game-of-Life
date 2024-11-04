package gol

type ReceiveFromServer struct {
	P            Params
	CurrentWorld [][]byte
}
type SendToClient struct {
	World [][]byte
}
