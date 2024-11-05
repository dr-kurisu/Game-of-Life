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

func aliveCounter(world [][]byte, p Params) int {
	result := 0
	var mutex sync.Mutex
	var wg sync.WaitGroup
	yInterval := p.ImageHeight / p.Threads
	ys := 0
	ye := yInterval
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
		ys += yInterval
		ye += yInterval
	}
	wg.Wait()
	return result
}

func checker(world [][]byte, p Params) []util.Cell {
	result := make([]util.Cell, 0)
	var mutex sync.Mutex
	var wg sync.WaitGroup
	yInterval := p.ImageHeight / p.Threads
	ys := 0
	ye := yInterval
	wg.Add(p.Threads)
	for i := 0; i < p.Threads; i++ {
		if i == p.Threads-1 {
			ye = p.ImageHeight
		}
		go func(ys, ye int) {
			defer wg.Done()
			localResult := make([]util.Cell, 0)
			for y := ys; y < ye; y++ {
				for x := 0; x < p.ImageWidth; x++ {
					if world[y][x] == byte(255) {
						localResult = append(localResult, util.Cell{X: x, Y: y})
					}
				}
			}
			mutex.Lock()
			result = append(result, localResult...)
			mutex.Unlock()
		}(ys, ye)
		ys += yInterval
		ye += yInterval
	}
	wg.Wait()
	return result
}

func output(world [][]byte, c *distributorChannels, p Params, turn int) string {
	Alive := 0
	c.ioCommand <- ioOutput
	filename := fmt.Sprintf("%dx%dx%d", p.ImageWidth, p.ImageHeight, turn)
	c.ioFilename <- filename
	for i := 0; i < p.ImageHeight; i++ {
		for j := 0; j < p.ImageWidth; j++ {
			if world[i][j] == byte(255) {
				Alive++
				c.ioOutput <- byte(255)
			} else {
				c.ioOutput <- byte(0)
			}
		}
	}
	fmt.Println("Output Alive:", Alive)
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
			*finalTurns = round
			return true
		case <-controlChan.ticker.C:
			c.events <- AliveCellsCount{CompletedTurns: round + 1, CellsCount: aliveCounter(world, p)}
		case <-controlChan.pause:
			pause = !pause
			if pause {
				c.events <- StateChange{round, Paused}
				quit = false
			} else {
				c.events <- StateChange{round, Executing}
				quit = true
			}
		case <-controlChan.save: // deal with saving
			Alive := aliveCounter(world, p)
			fmt.Println("checker Alive: ", Alive)
			fmt.Println("Current round:", round)
			filename := output(world, &c, p, round)
			c.ioCommand <- ioCheckIdle
			select {
			case <-c.ioIdle:
			}
			c.events <- ImageOutputComplete{CompletedTurns: round, Filename: filename}
		default:
		}
		if quit {
			break
		}
	}
	return false

}

func broker(world [][]byte, clients []*rpc.Client, turn int, p Params) [][]byte {
	yInterval := p.ImageHeight / len(clients)
	height := len(world)
	width := len(world[0])
	result := make([][]byte, height)
	for i := 0; i < height; i++ {
		result[i] = make([]byte, width)
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(clients))
	for i, client := range clients {
		startY := i * yInterval
		endY := startY + yInterval
		if i == len(clients)-1 {
			endY = height
		}
		request := Request{
			World: make([][]byte, 0),
			Turn:  turn,
			P:     p,
		}
		for j := startY; j < endY+2; j++ {
			request.World = append(request.World, world[(j-1+height)%height])
		}
		response := Response{}
		client := client
		go func() {
			defer wg.Done()
			err := client.Call("Operator.DistributeWorker", request, &response)
			if err != nil {
				panic(err)
			}
			for j := 0; j < len(response.World); j++ {
				for k := 0; k < len(response.World[j]); k++ {
					result[j][k] = response.World[j][k]
				}
			}
		}()
	}
	wg.Wait()
	return result
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

	// Receive world from IO
	for index := 0; index < p.ImageHeight*p.ImageWidth; index++ {
		world[index/p.ImageWidth][index%p.ImageWidth] = <-c.ioInput
	}
	// define of ports
	ports := make([]string, 1)
	ports[0] = "localhost:8030" // Ip to be confirmed
	// Send world to server
	clients := make([]*rpc.Client, len(ports))
	for i := 0; i < len(clients); i++ {
		client, err := rpc.Dial("tcp", ports[0])
		if err != nil {
			panic(err)
		}
		clients[i] = client
	}
	turn := 0
	finalTurns := p.Turns
	// TODO: Execute all turns of the Game of Life.
	// let server run the task and receive world in each round
	c.events <- StateChange{turn, Executing}
	GameQuit := false
	controlChan := controlChannels{make(chan bool, 1), make(chan bool, 1), make(chan bool), time.NewTicker(2 * time.Second)}
	go keyBoardDetection(keyPresses, controlChan)

	for i := 0; i < finalTurns; i++ {
		world = broker(world, clients, i, p)
		GameQuit = handlingEvent(&controlChan, c, &finalTurns, world, p, i)
		if GameQuit {
			fmt.Println("turn:", i)
			break
		}
	}
	// output as pgm file
	outputFileName := output(world, &c, p, finalTurns)
	c.ioCommand <- ioCheckIdle
	select {
	case <-c.ioIdle:
		c.events <- ImageOutputComplete{CompletedTurns: finalTurns, Filename: outputFileName}
	}
	// TODO: Report the final state using FinalTurnCompleteEvent.
	c.events <- FinalTurnComplete{p.Turns, checker(world, p)}

	// Make sure that the Io has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	c.events <- StateChange{turn, Quitting}

	// Close the channel to stop the SDL goroutine gracefully. Removing may cause deadlock.
	close(c.events)
}
