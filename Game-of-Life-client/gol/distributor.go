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
type controlChannels struct {
	pause  chan bool
	quit   chan bool
	save   chan bool
	ticker *time.Ticker
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
func comWithServer(client *rpc.Client, world [][]byte, p Params, group *sync.WaitGroup) []util.Cell {
	result := make([]util.Cell, 0)
	send := ReceiveFromServer{P: p, CurrentWorld: world}
	res := SendToClient{}
	res.World = make([][]byte, p.ImageHeight)
	for i := 0; i < p.ImageHeight; i++ {
		res.World[i] = make([]byte, p.ImageWidth)
		for j := 0; j < p.ImageWidth; j++ {
			res.World[i][j] = world[i][j]
		}
	}
	client.Call("Operator.Connection", send, &res)
	for q := 0; q < p.ImageHeight; q++ {
		for a := 0; a < p.ImageWidth; a++ {
			send.CurrentWorld[q][a] = res.World[q][a]
			if res.World[q][a] == 255 {
				result = append(result, util.Cell{X: q, Y: a})
			}
		}
	}
	return result
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
func keyBoardDetection(keyPresses <-chan rune, controlChan controlChannels) {
	for {
		key := <-keyPresses
		switch key {
		case 's':
			controlChan.save <- true
		case 'q':
			controlChan.quit <- true
		case 'p':
			controlChan.pause <- true
		default:
			panic("Wrong input from keyboard")
		}
	}
}
func handlingEvent(controlChan *controlChannels, c distributorChannels, finalTurns *int, world [][]byte, p Params, round int) bool {
	quit := true
	pause := false
	for {
		select {
		case <-controlChan.quit:
			*finalTurns = round + 1
			return true
		case <-controlChan.ticker.C:
			c.events <- AliveCellsCount{CompletedTurns: round + 1, CellsCount: aliveCounter(world, p)}
		case <-controlChan.pause:
			pause = !pause
			if pause {
				c.events <- StateChange{round, Paused}
				quit = false
			} else {
				c.events <- StateChange{round + 1, Executing}
				quit = true
			}
		case <-controlChan.save: // deal with saving
			filename := output(world, &c, p, round+1)
			c.ioCommand <- ioCheckIdle
			select {
			case <-c.ioIdle:
			}
			c.events <- ImageOutputComplete{CompletedTurns: round + 1, Filename: filename}
		default:
		}
		if quit {
			break
		}
	}
	return false

}

//broker
func broker(world [][]byte, p Params, c distributorChannels, keyPresses <-chan rune, server int, port []string) [][]byte {
	nextround := make([][]byte, p.ImageHeight)
	resultOfWorker := make([]util.Cell, p.ImageHeight*p.ImageWidth)
	for i := 0; i < p.ImageHeight; i++ {
		nextround[i] = make([]byte, p.ImageWidth)
	}
	var yInterval int
	if server == 1 {
		yInterval = p.ImageHeight
	} else {
		yInterval = (p.ImageHeight / server) + 2
	}
	for i := 0; i < p.Turns; i++ {
		separateWorld := make([][]byte, yInterval) // give top line and the bottom line
		for i := 0; i < yInterval; i++ {
			separateWorld[i] = make([]byte, p.ImageWidth)
		}
		var wg sync.WaitGroup
		for i := range port {
			wg.Add(1)
			client, _ := rpc.Dial("tcp", port[i])
			fmt.Println("connection started")
			if client == nil {
				fmt.Println("Wrong conn")
			}
			defer client.Close()
			go func() {
				defer wg.Done()
				resultOfWorker = append(resultOfWorker, comWithServer(client, world, p, &wg)...)
			}()
		}
		wg.Wait()
		GameQuit := false
		finalTurns := p.Turns
		controlChan := controlChannels{make(chan bool, 1), make(chan bool, 1), make(chan bool), time.NewTicker(2 * time.Second)}
		go keyBoardDetection(keyPresses, controlChan)
		GameQuit = handlingEvent(&controlChan, c, &finalTurns, world, p, i)
		if GameQuit {
			break
		}
		for l := range resultOfWorker {
			current := resultOfWorker[l]
			nextround[current.Y][current.X] = 255
		}
	}
	return nextround
}

// distributor divides the work between workers and interacts with other goroutines.
func distributor(p Params, c distributorChannels, keyPresses <-chan rune) {
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
	port := make([]string, 100)
	port[0] = "localhost:8030" // Ip to be confirmed
	// ........ more address
	finalTurns := p.Turns

	// communicate with server and get the response
	c.events <- StateChange{turn, Executing}
	broker(world, p, c, keyPresses, len(port), port)
	// output as pgm file
	outputFileName := output(world, &c, p, finalTurns)
	c.ioCommand <- ioCheckIdle
	select {
	case <-c.ioIdle:
		c.events <- ImageOutputComplete{CompletedTurns: finalTurns, Filename: outputFileName}
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
