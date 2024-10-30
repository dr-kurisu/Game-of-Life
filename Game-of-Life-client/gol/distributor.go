package gol

import (
	"fmt"
	"net/rpc"
	"sync"
	"time"
	"uk.ac.bris.cs/gameoflife/util"
)

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
func aliveCounter(world [][]byte, p Params) int {
	result := 0
	var mutex sync.Mutex
	var wg sync.WaitGroup
	y_interval := p.ImageHeight / p.Threads
	ys := 0
	ye := y_interval
	wg.Add(p.Threads)
	for i := 0; i < p.Threads; i++ {
		if i == p.Threads-1 {
			ye = p.ImageHeight
		}
		go func(ys, ye int) {
			defer wg.Done()
			localResult := 0
			for y := ys; y < ye; y++ {
				for x := 0; x < p.ImageWidth; x++ {
					if world[y][x] == byte(255) {
						localResult += 1
					}
				}
			}
			mutex.Lock()
			result += localResult
			mutex.Unlock()
		}(ys, ye)
		ys += y_interval
		ye += y_interval
	}
	wg.Wait()
	return result
}
func comWithServer(client *rpc.Client, world [][]byte, p Params, c distributorChannels) [][]byte {
	send := ReceiveFromServer{P: p, CurrentWorld: world}
	res := SendToClient{}
	res.World = make([][]byte, p.ImageHeight)
	for i := 0; i < p.ImageHeight; i++ {
		res.World[i] = make([]byte, p.ImageWidth)
		for j := 0; j < p.ImageWidth; j++ {
			res.World[i][j] = world[i][j]
		}
	}

	result := make([][]byte, p.ImageHeight)
	for i := 0; i < p.ImageHeight; i++ {
		result[i] = make([]byte, p.ImageWidth)
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	done := make(chan *rpc.Call, 1)
	for i := 0; i < p.Turns; i++ {
		client.Go("Operator.Connection", send, &res, done)
		quit := false
		for {
			select {
			case <-ticker.C:
				c.events <- AliveCellsCount{CompletedTurns: i, CellsCount: aliveCounter(send.CurrentWorld, p)}
			case <-done:
				quit = true
			default:
			}
			if quit {
				for q := 0; q < p.ImageHeight; q++ {
					for a := 0; a < p.ImageWidth; a++ {
						send.CurrentWorld[q][a] = res.World[q][a]
					}
				}
				break
			}
		}
	}
	return res.World
}

func output(world [][]byte, c *distributorChannels, p Params, turn int) string {
	c.ioCommand <- ioOutput
	filename := fmt.Sprintf("%dx%dx%d", p.ImageWidth, p.ImageHeight, turn)
	c.ioFilename <- filename
	for i := 0; i < p.ImageHeight; i++ {
		for j := 0; j < p.ImageWidth; j++ {
			c.ioOutput <- world[i][j]
		}
	}
	return filename
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
	// TODO: Execute all turns of the Game of Life.
	// initialise the client
	server := "localhost:8030" // Ip to be confirmed
	fmt.Println("connection started")
	client, _ := rpc.Dial("tcp", server)
	if client == nil {
		fmt.Println("Wrong conn")
	}
	defer client.Close()
	// communicate with server and get the response
	c.events <- StateChange{turn, Executing}
	world = comWithServer(client, world, p, c)
	// output as pgm file
	finalTurn := p.Turns
	outputFileName := output(world, &c, p, finalTurn)
	c.ioCommand <- ioCheckIdle
	select {
	case <-c.ioIdle:
		c.events <- ImageOutputComplete{CompletedTurns: finalTurn, Filename: outputFileName}
	}
	// TODO: Report the final state using FinalTurnCompleteEvent.
	c.events <- FinalTurnComplete{p.Turns, checker(world)}

	// Make sure that the Io has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	c.events <- StateChange{turn, Quitting}

	// Close the channel to stop the SDL goroutine gracefully. Removing may cause deadlock.
	close(c.events)
}
