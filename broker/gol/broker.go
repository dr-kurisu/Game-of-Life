package gol

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

type Operator struct{}

var pauseChan = make(chan bool, 1)
var quitChan = make(chan bool, 1)
var saveChan = make(chan bool, 1)
var ticker = make(chan bool, 2)
var pause = false
var worldChan = make(chan [][]byte)
var turn = 0
var threads = 1

func (a *Operator) BrokerWorker(request RequestBroker, response *ResponseBroker) error {
	// deal with connection
	clients := make([]*rpc.Client, len(request.Ports))
	for i := 0; i < len(clients); i++ {
		client, err := rpc.Dial("tcp", request.Ports[i])
		if err != nil {
			panic(err)
		}
		clients[i] = client
	}
	threads = request.P.Threads
	var world [][]byte
	finalTurns := request.P.Turns
	quit := false
	for i := 0; i < request.P.Turns; i++ {
		world = broker(request.World, clients, i, request.P)
		turn = i + 1
		for {
			select {
			case <-ticker:
				worldChan <- world
			case <-pauseChan:
				pause = !pause
			case <-quitChan:
				quit = true
			case <-saveChan:
				worldChan <- world
			default:
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

	response.World = make([][]byte, len(world))
	for i := range world {
		response.World[i] = make([]byte, len(world[i]))
		copy(response.World[i], world[i])
	}
	response.FinalTurns = finalTurns
	return nil
}

func (a *Operator) Pause(request NoParams, response *PauseResponse) error {
	pauseChan <- pause
	// Turn 有点问题
	response.Turn = turn
	return nil
}

func (a *Operator) Save(request NoParams, response *ResponseBroker) error {
	saveChan <- true
	world := <-worldChan
	response.World = make([][]byte, len(world))
	for i := range world {
		response.World[i] = make([]byte, len(world[i]))
		copy(response.World[i], world[i])
	}
	response.FinalTurns = turn
	return nil
}

func (a *Operator) Ticker(request NoParams, response *TickerResponse) error {
	ticker <- true
	world := <-worldChan
	nums := aliveCounter(world, threads)
	response.AliveNum = nums
	response.Turn = turn
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

func server() {
	err := rpc.Register(&Operator{})
	if err != nil {
		fmt.Println(err)
		return
	}
	listener, _ := net.Listen("tcp", ":8031")
	fmt.Println("Server is listening on port", "8031")
	rpc.Accept(listener)
}
