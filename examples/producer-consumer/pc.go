package main

import (
	"fmt"
	"time"
)

func producer(numToProduce int, buf chan *Product, pdone, cdone chan struct{}) {
	defer close(pdone)
	for i := range numToProduce {
		select {
		case buf <- &Product{ID: i}:
			//println("produced ", i)
		case <-cdone:
			return
		}
	}
}

func consumer(numToConsume int, buf chan *Product, pdone, cdone chan struct{}) {
	defer close(cdone)
	for range numToConsume {
		select {
		case x := <-buf:
			if x.ID > 100_000_000 {
				println("consumed ", x)
			}
		case <-pdone:
			return
		}
	}
}

type Product struct {
	ID int
}

func main() {
	buf := make(chan *Product, 2)
	numToProduce := 1_000_000
	numToConsume := numToProduce
	pdone := make(chan struct{})
	cdone := make(chan struct{})
	t0 := time.Now()
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
	fmt.Printf("elap = %v\n", time.Since(t0))
}
