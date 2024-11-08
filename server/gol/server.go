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

var world [][]byte

var dir = [][]int{{1, 0}, {-1, 0}, {-1, -1}, {1, 1}, {0, -1}, {1, -1}, {-1, 1}, {0, 1}}

var killChan = make(chan bool)

//these functions are used to simulate the turns of Game of Life
func singleThreadWorker(world, result [][]byte) {
	height := len(world)
	width := len(world[0])

	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			if world[y][x] == byte(255) {
				for _, d := range dir {
					result[y+d[0]+1][(x+d[1]+width)%width] += 1
				}
			}
		}
	}
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

func centralDistributor(world [][]byte, p Params) [][]byte {
	/*
		Received y, x world return y + 2, x result
	*/
	p.Threads = 1
	height := len(world) + 2
	width := len(world[0])

	result := make([][]byte, height)
	for i := 0; i < height; i++ {
		result[i] = make([]byte, width)
	}
	// wg used to control each thread
	singleThreadWorker(world, result)
	return result
}

func (a *Operator) Kill(request NoParams, response *NoParams) error {
	killChan <- true
	return nil
}

func (a *Operator) DistributeWorker(request Request, response *Response) error {
	world = centralDistributor(request.World, request.P)
	// send world
	response.World = make([][]byte, len(world))
	for i := range world {
		response.World[i] = make([]byte, len(world[i]))
		copy(response.World[i], world[i])
	}
	return nil
}

//It's the sever, used to listen and pass the argus to client by channel
func server(port string) {
	err := rpc.Register(&Operator{})
	if err != nil {
		fmt.Println(err)
		return
	}
	listener, _ := net.Listen("tcp", port)
	fmt.Println("Server is listening on port", port)
	go func() {
		<-killChan
		// simulate interrupt signal
		fmt.Println("--- received killed signal ---")
		err = syscall.Kill(os.Getpid(), syscall.SIGINT)
		if err != nil {
			panic(err)
		}
	}()
	rpc.Accept(listener)
}
