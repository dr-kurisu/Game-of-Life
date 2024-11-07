package gol

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"uk.ac.bris.cs/gameoflife/util"
)

type Operator struct{}

var pauseChan = make(chan bool, 1)
var quitChan = make(chan bool, 1)
var saveChan = make(chan bool, 1)
var killChan = make(chan bool)
var worldChan = make(chan [][]byte)
var ticker = make(chan bool, 1)
var comapreComplete = make(chan bool)
var turnComplete = make(chan bool, 1)

var pause = false
var kill = false
var turn = 0
var threads = 1
var clients []*rpc.Client
var differentCells []util.Cell

func (a *Operator) BrokerWorker(request RequestBroker, response *ResponseBroker) error {
	// deal with connection
	clients = make([]*rpc.Client, len(request.Ports))
	for i := 0; i < len(clients); i++ {
		client, err := rpc.Dial("tcp", request.Ports[i])
		if err != nil {
			panic(err)
		}
		clients[i] = client
	}
	// init of var
	threads = request.P.Threads
	world := make([][]byte, request.P.ImageHeight)
	for i := 0; i < len(world); i++ {
		world[i] = make([]byte, request.P.ImageWidth)
	}
	finalTurns := request.P.Turns
	quit := false
	for _, cell := range request.Cells {
		world[cell.Y][cell.X] = byte(255)
	}
	// start working with evolution
	for i := 0; i < request.P.Turns; i++ {

		turn = i + 1
		world = broker(world, clients, i, request.P)
		turnComplete <- true
		<-comapreComplete
		for {
			select {
			case <-ticker:
				fmt.Println("Received ticker signal")
				worldChan <- world
			case <-pauseChan:
				fmt.Println("Received pause signal")
				pause = !pause
			case <-quitChan:
				fmt.Println("Received quit signal")
				quit = true
			case <-saveChan:
				fmt.Println("Received save signal")
				worldChan <- world
			default:
			}
			if kill {
				return nil
			}
			if !pause || quit {
				break
			}
		}
		if quit {
			finalTurns = i + 1
			break
		}
	}
	response.Cells = make([]util.Cell, 0)
	for i := range world {
		for j := range world[i] {
			if world[i][j] == byte(255) {
				response.Cells = append(response.Cells, util.Cell{X: j, Y: i})
			}
		}
	}
	response.FinalTurns = finalTurns
	return nil
}

func (a *Operator) CellFlip(request NoParams, response *CellFlipResponse) error {
	<-turnComplete
	response.Turn = turn
	response.Cells = differentCells
	comapreComplete <- true
	return nil
}

func (a *Operator) Pause(request NoParams, response *PauseResponse) error {
	pauseChan <- pause
	response.Turn = turn
	return nil
}

func (a *Operator) Save(request NoParams, response *ResponseBroker) error {
	saveChan <- true
	world := <-worldChan
	response.Cells = make([]util.Cell, 0)
	for i := range world {
		for j := range world[i] {
			if world[i][j] == byte(255) {
				response.Cells = append(response.Cells, util.Cell{X: i, Y: j})
			}
		}
	}
	response.FinalTurns = turn
	return nil
}

func (a *Operator) Ticker(request NoParams, response *TickerResponse) error {
	ticker <- true
	response.Turn = turn
	world := <-worldChan
	nums := aliveCounter(world, threads)
	response.AliveNum = nums
	return nil
}

func (a *Operator) Kill(request NoParams, response *NoParams) error {
	killChan <- true
	kill = true
	return nil
}

func (a *Operator) Quit(request NoParams, response *NoParams) error {
	quitChan <- true
	return nil
}

func aliveCounter(world [][]byte, threads int) int {
	result := 0
	var mutex sync.Mutex
	var wg sync.WaitGroup
	yInterval := len(world) / threads
	ys := 0
	ye := yInterval
	wg.Add(threads)
	for i := 0; i < threads; i++ {
		if i == threads-1 {
			ye = len(world)
		}
		go func(ys, ye int) {
			defer wg.Done()
			localResult := 0
			for y := ys; y < ye; y++ {
				for x := 0; x < len(world[0]); x++ {
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
				if kill {
					return
				} else {
					panic(err)
				}
			}
			for j := 0; j < len(response.World); j++ {
				for k := 0; k < len(response.World[j]); k++ {
					result[j][k] = response.World[j][k]
				}
			}
		}()
	}
	wg.Wait()
	// compare result and world
	differentCells = make([]util.Cell, 0)
	cellFlipHandling(world, result, p)
	return result
}

func cellFlipHandling(world [][]byte, result [][]byte, p Params) {
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			if world[y][x] != byte(255) && result[y][x] == byte(255) || world[y][x] == byte(255) && result[y][x] != byte(255) {
				differentCells = append(differentCells, util.Cell{X: x, Y: y})
			}
		}
	}
}

func server() {
	err := rpc.Register(&Operator{})
	if err != nil {
		fmt.Println(err)
		return
	}
	listener, _ := net.Listen("tcp", ":8031")
	fmt.Println("Broker is listening on port", "8031")
	go func() {
		<-killChan
		// tell servers to close as well
		for _, client := range clients {
			err = client.Call("Operator.Kill", struct{}{}, nil)
			if err != nil {
				panic(err)
			}
		}
		// simulate interrupt signal
		fmt.Println("--- received killed signal ---")
		err = syscall.Kill(os.Getpid(), syscall.SIGINT)
		if err != nil {
			panic(err)
		}
	}()
	rpc.Accept(listener)

}
