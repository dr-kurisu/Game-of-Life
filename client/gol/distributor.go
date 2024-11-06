package gol

import (
	"fmt"
	"net/rpc"
	"sort"
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
	kill   chan bool
	ticker *time.Ticker
}

type brokerCommunicate struct {
	broker   *rpc.Client
	call     *rpc.Call
	response *ResponseBroker
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
	c.ioCommand <- ioOutput
	filename := fmt.Sprintf("%dx%dx%d", p.ImageWidth, p.ImageHeight, turn)
	c.ioFilename <- filename
	for i := 0; i < p.ImageHeight; i++ {
		for j := 0; j < p.ImageWidth; j++ {
			if world[i][j] == byte(255) {
				c.ioOutput <- byte(255)
			} else {
				c.ioOutput <- byte(0)
			}
		}
	}
	return filename
}

func outputWithCells(cells []util.Cell, c *distributorChannels, p Params, turn int) string {
	c.ioCommand <- ioOutput
	filename := fmt.Sprintf("%dx%dx%d", p.ImageWidth, p.ImageHeight, turn)
	c.ioFilename <- filename
	// sort cells in order of Y and second order of X
	sort.Slice(cells, func(i, j int) bool {
		if cells[i].Y != cells[j].Y {
			return cells[i].Y < cells[j].Y
		}
		return cells[i].X < cells[j].X
	})
	counter := 0
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			if counter < len(cells) && cells[counter].Y == y && cells[counter].X == x {
				c.ioOutput <- byte(255)
				counter++
			} else {
				c.ioOutput <- byte(0)
			}
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
		case 'k':
			controlChan.kill <- true
		default:
			panic("Wrong input from keyboard")
		}
	}
}

func handlingEvent(controlChan *controlChannels, c distributorChannels, p Params, brokerC *brokerCommunicate) {
	pause := false
	broker := brokerC.broker
	call := brokerC.call
	go func() {
		for {
			response := CellFlipResponse{}
			err := broker.Call("Operator.CellFlip", struct{}{}, &response)
			if err != nil {
				panic(err)
			}
			c.events <- CellsFlipped{
				CompletedTurns: response.Turn,
				Cells:          response.Cells,
			}
			c.events <- TurnComplete{CompletedTurns: response.Turn}
		}
	}()
	for {
		select {
		case <-call.Done:
			// when broker is done
			fmt.Println("Broker is done")
			cells := brokerC.response.Cells
			finalTurns := brokerC.response.FinalTurns
			c.events <- FinalTurnComplete{finalTurns, cells}
			filename := outputWithCells(cells, &c, p, finalTurns)
			c.ioCommand <- ioCheckIdle
			<-c.ioIdle
			c.events <- ImageOutputComplete{
				CompletedTurns: brokerC.response.FinalTurns,
				Filename:       filename,
			}
			return

		case <-controlChan.quit:
			fmt.Println("Quiting")
			err := broker.Call("Operator.Quit", struct{}{}, &struct{}{})
			if err != nil {
				panic(err)
			}
			return
		case <-controlChan.kill:
			fmt.Println("Killing")
			err := broker.Call("Operator.Kill", struct{}{}, &struct{}{})
			if err != nil {
				return
			}
			return

		case <-controlChan.ticker.C:
			response := TickerResponse{}
			err := broker.Call("Operator.Ticker", struct{}{}, &response)
			if err != nil {
				panic(err)
			}
			c.events <- AliveCellsCount{CompletedTurns: response.Turn, CellsCount: response.AliveNum}
		case <-controlChan.pause:
			pause = !pause
			if pause {
				fmt.Println("Continuing")
				response := PauseResponse{}
				err := broker.Call("Operator.Pause", struct{}{}, &response)
				if err != nil {
					panic(err)
				}
				c.events <- StateChange{response.Turn, Paused}
			} else {
				fmt.Println("Continue")
				response := PauseResponse{}
				err := broker.Call("Operator.Pause", struct{}{}, &response)
				if err != nil {
					panic(err)
				}
				c.events <- StateChange{response.Turn, Executing}
			}
		case <-controlChan.save:
			response := ResponseBroker{}
			result := make([][]byte, p.ImageHeight)
			for y := 0; y < p.ImageHeight; y++ {
				result[y] = make([]byte, p.ImageWidth)
			}
			err := broker.Call("Operator.Save", struct{}{}, &response)
			if err != nil {
				panic(err)
			}
			// assign result used to output
			for _, cell := range response.Cells {
				result[cell.Y][cell.X] = byte(255)
			}
			finalTurns := response.FinalTurns

			filename := output(result, &c, p, response.FinalTurns)
			c.ioCommand <- ioCheckIdle
			select {
			case <-c.ioIdle:
			}
			c.events <- ImageOutputComplete{CompletedTurns: finalTurns, Filename: filename}
		}
	}
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
	// Flip cells
	c.events <- CellsFlipped{0, checker(world, p)}

	// define of ports
	ports := make([]string, 1)
	ports[0] = "localhost:8030" // Ip to be confirmed
	// Send world to server
	broker, err := rpc.Dial("tcp", "localhost:8031")
	if err != nil {
		panic(err)
	}

	turn := 0
	// TODO: Execute all turns of the Game of Life.
	// let server run the task and receive world in each round
	c.events <- StateChange{turn, Executing}
	controlChan := controlChannels{make(chan bool, 1), make(chan bool, 1), make(chan bool), make(chan bool), time.NewTicker(2 * time.Second)}
	go keyBoardDetection(keyPresses, controlChan)

	request := RequestBroker{
		Cells: checker(world, p),
		Ports: ports,
		P:     p,
	}

	response := ResponseBroker{}
	call := broker.Go("Operator.BrokerWorker", request, &response, nil)
	brokerC := brokerCommunicate{}
	brokerC.broker = broker
	brokerC.call = call
	brokerC.response = &response
	handlingEvent(&controlChan, c, p, &brokerC)
	//fmt.Println("Quit")

	// Make sure that the Io has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle
	c.events <- StateChange{turn, Quitting}
	// Close the channel to stop the SDL goroutine gracefully. Removing may cause deadlock.
	close(c.events)
}
