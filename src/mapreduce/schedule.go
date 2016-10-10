package mapreduce

import (
	"fmt"
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
	var taskmode_array = make([]TaskMode, ntasks + 1)
	// error : v declared and not used 
	// for _, v := range isTaskDone {
	// 	v = false
	// }
	for i := 0; i < ntasks; i++{
		taskmode_array[i].mode = 0
	}
	var allDone bool
	for{
		// ZW: Highlevel idea.
		//     Basically the infinite loop just check if there is any task not finished (either mode == 0 or mode == 1)
		//     If all tasks are finished, it breaks out the infinite loop and returns.
		//     If not, issue the task (mode == 0) using go routine with a anonymous function.
		//     In the anonymous function, it 
		//     1. Initialize the do task arguments.
		//     2. Fetch a free worker from the registerChannel.
		//     3. Call the worker. When it's done, update the task mode and clean up.
		// ZW: TIPS:
		//     1. Use debug function. Check common.go and set debugEnabled to true.
		allDone = true
		for i := 0; i < ntasks; i+=1 {
			if taskmode_array[i].mode == 2 || taskmode_array[i].mode == 1{
				//the task i is done or being processed 
				// ZW: If it's being processed, then allDone should also be false.
				//     With this line we don't have to check if allDone every time a task is done.
				// if taskmode_array[i].mode == 1 {
				//     allDone = false
				// }
				continue
			}else{
				allDone = false
				fmt.Printf("task %v , phase %v , nios %v \n", i, phase, nios)
				// ZW: Change the name from registerChannel to worker is much better. 'registerChannel' is
				//     not a channel, that's confusing.
				// ZW: Also this will block until there is a free worker, which will slow down this for loop.
				//     Instead, move this line into the labmda function below.
				registerChannel := <- mr.registerChannel
				// ZW: This lambda function is a closure, which means most of the parameters you send to it are not necessary.
				//     Try remove all the parameters except the task number.
				//     We have keep task number passed as a parameter because in go
				//     closure captures reference, not value.
				//     Most of the parameters like nios, phase will not change, therefore we
				//     can let closure captures it.
				//     But with i, it will be changed by outer loop.
				//     Suppose you issued task (i = 99), and then the outer loop increments
				//     i to 100, you will have a runtime error because in the go routine,
				//     it will try to update task 100.
				//     Believe me this is why your code trys to do task 100. If you check your
				//     code, although you pass tasknumber as a parameter, you accidently use
				//     taskmode_array[i].mode = 2.
				go func(tasknumber int, nios int, phase jobPhase, registerChannel string){
					
					// ZW: Again, get the free worker here to avoid slowing down the outside loop.
					
					fmt.Printf("registerChannel %v \n", registerChannel)
					
					// ZW: Do not update taskmode_array here because this may cause the problem that one task get issued multiple times.
					//     Suppose task 0 is free, and the scheduler just issue it with the go routine.
					//     There is possibility that before the go routine actually set task 0 mode to 1,
					//     the scheduler just runs another loop and found task 0 is free and issue another go routine.
					//     Of course this may not happen in most situation, but there is possibility.
					//     So instead of set task mode to 1 in the go routine, do it in the scheduler before issuing the go routine.
					// ZW: I have to comment more about this issue.
					//     Such conrner case happens when you have multiple threads trying to update a piece of data.
					//     So you should avoid data sharing by using channel. However just try to follow my comments and fix you code first.
					taskmode_array[i].tasknumber = i
					taskmode_array[i].mode = 1
					taskmode_array[i].workername = registerChannel
					
					// ZW: This is ok. But try to initialize DoTaskArgs first and then call.
					//     Line should be short and concise.
					isDone := call(registerChannel, "Worker.DoTask", &DoTaskArgs{mr.jobName, mr.files[tasknumber], phase, tasknumber, nios}, &struct{}{})
					fmt.Printf("task %v is %v\n", i, isDone)
					
					// ZW: So far, so good. But the following code is truly mysterious...
					
					if(isDone){
						go func(){
							// ZW: Yes you are right, use a go routine to return the worker, since 
							//     it will block if there is no consumer.
							mr.registerChannel <- registerChannel
							fmt.Printf("registerChannel %v go back to channel\n", registerChannel)
							
							// ZW: NOOOOOO...
							//     This thread will blocked until it successfully push the worker back
							//     into the register channel, which means your taskmode_array[i].mode will
							//     not be updated until that.
							//     Update the task mode before the go routine.
							taskmode_array[i].mode = 2
							fmt.Printf("task %v done\n", i)
							
							// ZW: Remove this line.
							allDone = isAllDone(taskmode_array, ntasks)
						}()
					}else{
						// ZW: You donot need a go routine here, nothing will block it right?
						//     Simply update the task array.
						go func(){
							taskmode_array[i].mode = 0
							fmt.Printf("task %v failed! re assigned\n", i)	
							
							// ZW: Remove this line.
							allDone = isAllDone(taskmode_array, ntasks)						
						}()
					}
				}(i, nios, phase, registerChannel) // ZW:  No arguments here except i.
			}	
			
			// ZW: Well remove this.
			if(!allDone){
				for i := 0; i < ntasks; i++{
					if(taskmode_array[i].mode != 2){
						fmt.Printf("task %v has not done yet\n", i)
					}
				}
			}
		}
		// Remove this.
		fmt.Printf("isAllDone ? %v\n", allDone)
		if allDone{
			break
		}			
	}

	// ZW: Remove this.
	for i := 0; i < ntasks; i++{
		if(taskmode_array[i].mode != 2){
			fmt.Printf("task %v has not done yet\n", i)
		}
	}
	fmt.Printf("Schedule: %v phase done\n", phase)
}

// ZW: We don't need it anymore.
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


// ZW: We don't need it anymore.
func scheduleParallel(mr *Master, phase jobPhase, nios int, t TaskMode, tk chan TaskMode){
	isDone := call(t.workername, "Worker.DoTask", &DoTaskArgs{mr.jobName, mr.files[t.tasknumber], phase, t.tasknumber, nios}, struct{}{})
	if(isDone){
		t.mode = 2
	}else{
		t.mode = 0
	}
	tk <- t
}
