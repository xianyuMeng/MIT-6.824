package mapreduce

import (
	"fmt"
	//"sync"
	)
type TaskMode struct{
	tasknumber int
	mode int
	//0 for not assigned, 1 for being processed, 2 for done
	workername string
}
// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	//dynamic allocate a bool array and init
	//var isTaskDone = make( []bool, ntasks)
	//var tasksmode = make(chan TaskMode)
	var taskmode_array = make([]TaskMode, ntasks)
	// error : v declared and not used 
	// for _, v := range isTaskDone {
	// 	v = false
	// }
	for i := 0; i < ntasks; i++{
		taskmode_array[i].mode = 0
	}
	var allDone bool
	//allDone is initialize to false
	if !allDone {
		for i := 0; i < ntasks; i++ {
			if taskmode_array[i].mode == 2 || taskmode_array[i].mode == 1 {
				continue
				//the task i is done or being processed 
			}else {
				fmt.Printf("task %v , phase %v , nios %v \n", i, phase, nios)
				registerChannel := <- mr.registerChannel
				go func(tasknumber int, nios int, phase jobPhase, registerChannel string){
					fmt.Printf("registerChannel %v \n", registerChannel)
					taskmode_array[i].tasknumber = i
					taskmode_array[i].mode = 1
					taskmode_array[i].workername = registerChannel
					isDone := call(registerChannel, "Worker.DoTask", &DoTaskArgs{mr.jobName, mr.files[tasknumber], phase, tasknumber, nios}, &struct{}{})
					fmt.Printf("task %v is %v\n", i, isDone)
					if(isDone){
						go func(){
							mr.registerChannel <- registerChannel
							fmt.Printf("registerChannel %v go back to channel\n", registerChannel)
						}()
						taskmode_array[i].mode = 2
						fmt.Printf("task %v done\n", i)
					}else{
						taskmode_array[i].mode = 0
						fmt.Printf("task %v failed\n", i)
					}	
				}(i, nios, phase, registerChannel)

				allDone = isAllDone(taskmode_array, ntasks)
				fmt.Printf("isAllDone ? %v\n", allDone)
			}
		}		
	}


	fmt.Printf("Schedule: %v phase done\n", phase)
}

func isAllDone(taskmode_array [] TaskMode, ntasks int) bool {
	var allDone bool
	allDone = true
	for i := 0; i < ntasks; i++{
		if taskmode_array[i].mode != 2 {
			allDone = false
		}
	}
	return allDone
}



func scheduleParallel(mr *Master, phase jobPhase, nios int, t TaskMode, tk chan TaskMode){
	isDone := call(t.workername, "Worker.DoTask", &DoTaskArgs{mr.jobName, mr.files[t.tasknumber], phase, t.tasknumber, nios}, struct{}{})
	if(isDone){
		t.mode = 2
	}else{
		t.mode = 0
	}
	tk <- t
}