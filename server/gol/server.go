package gol

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

type Operator struct{}

var world [][]byte

var dir = [][]int{{1, 0}, {-1, 0}, {-1, -1}, {1, 1}, {0, -1}, {1, -1}, {-1, 1}, {0, 1}}

//these functions are used to simulate the turns of Game of Life
func worker(partWorld [][]byte, partResult [][]byte, wg *sync.WaitGroup) {
	defer wg.Done()
	interval := len(partResult)
	width := len(partResult[0])
	for y := 0; y < interval; y++ {
		for x := 0; x < width; x++ {
			for _, d := range dir {
				if partWorld[y+d[0]+1][(x+d[1]+width)%width] == byte(255) {
					partResult[y][x] += 1
				}
			}
		}
	}
	for y := 0; y < interval; y++ {
		for x := 0; x < width; x++ {
			if partResult[y][x] == byte(3) || (partResult[y][x] == byte(2) && partWorld[y+1][x] == byte(255)) {
				partResult[y][x] = byte(255)
			}
		}
	}
}

func centralDistributor(world [][]byte, p Params, turn int) [][]byte {
	height := len(world) - 2
	width := len(world[0])

	yInterval := height / p.Threads

	result := make([][]byte, height)
	for i := 0; i < height; i++ {
		result[i] = make([]byte, width)
	}
	// wg used to control each thread
	wg := sync.WaitGroup{}
	wg.Add(p.Threads)
	for i := 0; i < p.Threads; i++ {
		startY := i * yInterval
		endY := startY + yInterval
		// deal with the last thread
		if i == p.Threads-1 {
			endY = height
		}
		partResult := result[startY:endY]
		// deal with start and medium
		go worker(world, partResult, &wg)
		//cellFlipHandling(world, result, p, c, turn)
	}
	wg.Wait()
	return result
}

func (a *Operator) DistributeWorker(request Request, response *Response) error {
	world = centralDistributor(request.World, request.P, request.Turn)
	// send world
	response.World = make([][]byte, len(world))
	for i := range world {
		response.World[i] = make([]byte, len(world[i]))
		copy(response.World[i], world[i])
	}
	return nil
}

//It's the sever, used to listen and pass the argus to client by channel
func server() {
	err := rpc.Register(&Operator{})
	if err != nil {
		fmt.Println(err)
		return
	}
	listener, _ := net.Listen("tcp", ":8030")
	fmt.Println("Server is listening on port", "8030")
	rpc.Accept(listener)
}
