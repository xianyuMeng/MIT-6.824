package mapreduce

import (
    "fmt"
)

type TaskMode struct {
    tasknumber int
    mode       int
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
    allDone := false
    // error : v declared and not used
    // for _, v := range isTaskDone {
    //  v = false
    // }
    for i := 0; i < ntasks; i++ {
        taskmode_array[i].mode = 0
    }
    for {
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
        for i := 0; i < ntasks; i++ {
            if taskmode_array[i].mode == 2 || taskmode_array[i].mode == 1 {
                //the task i is done or being processed
                // ZW: If it's being processed, then allDone should also be false.
                //     With this line we don't have to check if allDone every time a task is done.
                if taskmode_array[i].mode == 1 {
                    allDone = false
                }
                continue
            } else {
                allDone = false
                fmt.Printf("task %v , phase %v , nios %v \n", i, phase, nios)
                taskmode_array[i].tasknumber = i
                taskmode_array[i].mode = 1

                go func(tasknumber int){
                    worker := <- mr.registerChannel
                    fmt.Printf("worker %v \n", worker)
                    // debug("task %v\n", i)
                    args := DoTaskArgs {
                        mr.jobName,
                        mr.files[tasknumber], 
                        phase, 
                        tasknumber, 
                        nios,
                    }
                    isDone := call(worker, "Worker.DoTask", &args, &struct{}{})
                    fmt.Printf("task %v is %v\n", tasknumber, isDone)
                    if isDone {
                        taskmode_array[tasknumber].mode = 2
                        go func() {
                            mr.registerChannel <- worker
                        }()
                    } else {
                        taskmode_array[tasknumber].mode = 0
                        fmt.Printf("task %v failed! re assigned\n", tasknumber)
                    }
                    //j := 1
                }(i)
            }
        }
        if allDone {
            break
        }
    }

    fmt.Printf("Schedule: %v phase done\n", phase)
}

// ZW: We don't need it anymore.
// func isAllDone(taskmode_array []TaskMode, ntasks int) bool {
//     var allDone bool
//     allDone = true
//     for i := 0; i < ntasks; i++ {
//         if taskmode_array[i].mode != 2 {
//             allDone = false
//         }
//     }
//     return allDone
// }

// ZW: We don't need it anymore.
// func scheduleParallel(mr *Master, phase jobPhase, nios int, t TaskMode, tk chan TaskMode) {
//     isDone := call(t.workername, "Worker.DoTask", &DoTaskArgs{mr.jobName, mr.files[t.tasknumber], phase, t.tasknumber, nios}, struct{}{})
//     if isDone {
//         t.mode = 2
//     } else {
//         t.mode = 0
//     }
//     tk <- t
// }