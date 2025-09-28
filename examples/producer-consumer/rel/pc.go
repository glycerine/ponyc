package main

import (
	"fmt"
	"time"
)

func producer(numToProduce int, buf chan int, pdone, cdone chan struct{}) {
	defer close(pdone)
	for i := range numToProduce {
		select {
		case buf <- i:
			//println("produced ", i)
		case <-cdone:
			return
		}
	}
}

func consumer(numToConsume int, buf chan int, pdone, cdone chan struct{}) {
	defer close(cdone)
	for range numToConsume {
		select {
		case x := <-buf:
			_ = x
			//println("consumed ", x)
		case <-pdone:
			return
		}
	}
}

func main() {
	t0 := time.Now()
	buf := make(chan int, 2)
	numToProduce := 1_000_000
	numToConsume := numToProduce
	pdone := make(chan struct{})
	cdone := make(chan struct{})
	go producer(numToProduce, buf, pdone, cdone)
	go consumer(numToConsume, buf, pdone, cdone)
	select {
	case <-pdone:
		pdone = nil
	case <-cdone:
		cdone = nil
	}
	select {
	case <-pdone:
	case <-cdone:
	}
	fmt.Printf("elap: %v\n", time.Since(t0))
}
