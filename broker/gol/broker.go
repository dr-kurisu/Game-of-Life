package gol

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

type Operator struct{}

// _________________________________change_____________________________________
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

	world := broker(request.World, clients, request.Turn, request.P)
	response.World = make([][]byte, len(world))
	for i := range world {
		response.World[i] = make([]byte, len(world[i]))
		copy(response.World[i], world[i])
	}
	return nil
}

// __________________________________change____________________________________________

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
