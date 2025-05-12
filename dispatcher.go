package main

import (
	"fmt"
	"log"
	"math"

	"ergo.services/ergo"
	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
)

type TaskDispatcher struct {
	act.Actor
	numberOfWorkers int
}

func factoryTaskDispatcher() gen.ProcessBehavior {
	return &TaskDispatcher{}
}

type CalculatePrimes struct {
	RangeStart int
	RangeEnd   int
}

type PrimesFound struct {
	Primes []int
}

func (td *TaskDispatcher) Init(args ...any) error {
	if len(args) > 0 {
		td.numberOfWorkers = args[0].(int)
	} else {
		// default number of workers to 4
		td.numberOfWorkers = 4
	}
	return nil
}

func (td *TaskDispatcher) HandleMessage(from gen.PID, message any) error {
	switch msg := message.(type) {
	case CalculatePrimes:
		for i := range td.numberOfWorkers {
			rangeSize := (msg.RangeEnd - msg.RangeStart + 1) / td.numberOfWorkers
			workerRangeStart := msg.RangeStart + i*rangeSize
			workerRangeEnd := workerRangeStart + rangeSize - 1

			pid, err := td.Spawn(
				func() gen.ProcessBehavior {
					return &Worker{}
				}, gen.ProcessOptions{})
			if err != nil {
				log.Fatalf("failed to spawn worker actor: %v", err)
			}

			td.Send(pid, CalculatePrimes{workerRangeStart, workerRangeEnd})
			if err != nil {
				log.Fatalf("failed to send messag to worker actor: %v", err)
			}
		}
	default:
		td.Log().Error("Unknown message: %v", msg)
	}

	return nil
}

type Worker struct {
	act.Actor
	RangeStart int
	RangeEnd   int
}

func (w *Worker) HandleMessage(from gen.PID, message any) error {
	switch msg := message.(type) {
	case CalculatePrimes:
		primes := findPrimesInRange(msg.RangeStart, msg.RangeEnd)
		err := w.Send(gen.Atom("ResultsCollector"), PrimesFound{primes})
		if err != nil {
			log.Fatalf("failed to send messag to worker actor: %v", err)
		}

		return gen.TerminateReasonNormal
	default:
		w.Log().Error("Unknown message: %v", msg)
	}

	return nil
}

func findPrimesInRange(start, end int) []int {
	var primes []int
	if start < 2 {
		start = 2
	}
	for num := start; num <= end; num++ {
		if isPrime(num) {
			primes = append(primes, num)
		}
	}

	return primes
}

func isPrime(n int) bool {
	if n <= 1 {
		return false
	}
	if n <= 3 {
		return true
	}
	if n%2 == 0 || n%3 == 0 {
		return false
	}

	sqrtN := int(math.Sqrt(float64(n)))
	for i := 5; i <= sqrtN; i += 6 {
		if n%i == 0 || n%(i+2) == 0 {
			return false
		}
	}

	return true
}

type ResultsCollector struct {
	act.Actor
	expectedNumberOfResults int
	numberOfResults         int
	results                 []int
}

func (rc *ResultsCollector) Init(args ...any) error {
	if len(args) > 0 {
		rc.expectedNumberOfResults = args[0].(int)
	} else {
		// default to 4
		rc.expectedNumberOfResults = 4
	}
	return nil
}

func (rc *ResultsCollector) HandleMessage(from gen.PID, message any) error {
	switch msg := message.(type) {
	case PrimesFound:
		rc.results = append(rc.results, msg.Primes...)
		rc.numberOfResults = rc.numberOfResults + 1

		if rc.numberOfResults == rc.expectedNumberOfResults {
			fmt.Printf("All results collected. Found the following primes: %v", msg.Primes)
		}
	default:
		rc.Log().Error("Unknown message: %v", msg)
	}

	return nil
}

func main() {
	name := gen.Atom("example@localhost")
	node, err := ergo.StartNode(name, gen.NodeOptions{})
	if err != nil {
		log.Fatalf("Failed to start the Ergo node: %v", err)
	}

	tdPid, err := node.Spawn(factoryTaskDispatcher, gen.ProcessOptions{}, 5)
	if err != nil {
		log.Fatalf("Failed to spawm the task dispatcher: %v", err)
	}

	_, err = node.SpawnRegister(gen.Atom("ResultsCollector"), func() gen.ProcessBehavior { return &ResultsCollector{} }, gen.ProcessOptions{}, 5)
	if err != nil {
		log.Fatalf("Failed to spawm the task dispatcher: %v", err)
	}

	node.Send(tdPid, CalculatePrimes{0, 100})

	node.Wait()
}
