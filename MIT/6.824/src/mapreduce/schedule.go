package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var nOther int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		nOther = nReduce
	case reducePhase:
		ntasks = nReduce
		nOther = len(mapFiles)
	}
	var wg sync.WaitGroup
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nOther)
	for taskCounter := 0; taskCounter < ntasks; taskCounter++ {
		wg.Add(1)
		go func(taskNum int) {
			for {
				srv := <-registerChan // 拿 address 必须放在 for-loop 里面, 因为拿到的 srv 可能已经挂掉了, 所以每次都要拿新的
				var inputFile string
				if phase == mapPhase {
					inputFile = mapFiles[taskNum]
				} else {
					inputFile = ""
				}
				newTask := DoTaskArgs{jobName, inputFile, phase, taskNum, nOther}
				res := call(srv, "Worker.DoTask", newTask, nil)
				if res { // task might failed
					wg.Done()
					registerChan <- srv // 放 address 必须在 wg.done() 之后, 没有想清楚为什么, 一个可能的原因是 registerChan 没有 buffer.
					break
				}
			}
		}(taskCounter)
	}
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
