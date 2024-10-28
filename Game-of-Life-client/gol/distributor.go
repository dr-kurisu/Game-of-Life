package gol

import (
	"fmt"
	"net/rpc"
	"uk.ac.bris.cs/gameoflife/util"
)

var GolWorld [][]byte

type distributorChannels struct {
	events     chan<- Event
	ioCommand  chan<- ioCommand
	ioIdle     <-chan bool
	ioFilename chan<- string
	ioOutput   chan<- uint8
	ioInput    <-chan uint8
}

func checker(world [][]byte) []util.Cell {
	result := make([]util.Cell, 0)
	for y, _ := range world {
		for x, _ := range world[y] {
			if world[y][x] == byte(255) {
				result = append(result, util.Cell{X: x, Y: y})
			}
		}
	}
	return result
}
func comWithServer(client *rpc.Client, world [][]byte, p Params) [][]byte {
	send := ReceiveFromServer{P: p, CurrentWorld: world}
	res := SendToClient{}
	res.World = make([][]byte, p.ImageHeight)
	for i := 0; i < p.ImageHeight; i++ {
		res.World[i] = make([]byte, p.ImageWidth)
		for j := 0; j < p.ImageWidth; j++ {
			res.World[i][j] = world[i][j]
		}
	}
	if len(checker(res.World)) == 0 {
		panic("23455")
	}
	client.Call("Operator.ConnectionHandler", send, &res)
	return res.World
}

// distributor divides the work between workers and interacts with other goroutines.
func distributor(p Params, c distributorChannels) {
	world := make([][]byte, p.ImageHeight)
	for i := 0; i < p.ImageHeight; i++ {
		world[i] = make([]byte, p.ImageWidth)
	}
	c.ioCommand <- ioInput
	filename := fmt.Sprintf("%dx%d", p.ImageWidth, p.ImageHeight)
	c.ioFilename <- filename

	for index := 0; index < p.ImageHeight*p.ImageWidth; index++ {
		world[index/p.ImageWidth][index%p.ImageWidth] = <-c.ioInput
	}
	turn := 0
	c.events <- StateChange{turn, Executing}
	// TODO: Execute all turns of the Game of Life.
	// initialise the client
	server := "localhost:8030" // Ip to be confirmed
	fmt.Println("connection started")
	client, _ := rpc.Dial("tcp", server)
	if client == nil {
		fmt.Println("Wrong conn")
	}
	defer client.Close()
	// creat a ticker for every 2 seconds
	//ticker := time.NewTicker(2 * time.Second)
	world = comWithServer(client, world, p)
	// TODO: Report the final state using FinalTurnCompleteEvent.
	c.events <- FinalTurnComplete{p.Turns, checker(world)}

	// Make sure that the Io has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	c.events <- StateChange{turn, Quitting}

	// Close the channel to stop the SDL goroutine gracefully. Removing may cause deadlock.
	close(c.events)
}
