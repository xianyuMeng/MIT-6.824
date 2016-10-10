package mapreduce

import (
	"hash/fnv"
	"encoding/json"
	"io/ioutil"
	"os"
	"fmt"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!



	// KeyValueMap := make([string]string)


	
	// filebyte, err:= ioutil.ReadFile(inFile)
	// if err != nil{
	// 	fmt.Println("failed read file %v\n", inFile)
	// }
	// KeyValueMap := mapF(inFile, string(filebyte))
	// //KeyValueMap is an array of type KeyValue
	// KeyValueHash := make(map[int] KeyValue)
	// for _, v := range KeyValueMap {
	// 	ReducedTaskNumber := int (ihash(v.Key)) % nReduce
	// 	KeyValueHash[ReducedTaskNumber] = v
	// } 

	// for k := range KeyValueHash{
	// 	filename := reduceName(jobName, mapTaskNumber, k)
	// 	file, err:= os.Create(filename)
	// 	if err != nil{
	// 		fmt.Println("failed create %v\n", filename)
	// 	}
	// 	defer file.Close()
	// 	enc := json.NewEncoder(file)
	// 	err = enc.Encode(&k)
	// }



	filebyte, err:= ioutil.ReadFile(inFile)
	if err != nil{
		fmt.Println("failed read file %v\n", inFile)
	}

	
	kv := mapF(inFile,string(filebyte))
	fileP := make([] *json.Encoder, nReduce)
	for i := 0; i < nReduce; i++{
		filename := reduceName(jobName, mapTaskNumber, i)
		file, err := os.Create(filename)
		if err != nil{
			fmt.Printf("failed to create file %v\n", filename)
		}else{
			enc := json.NewEncoder(file)
			fileP[i] = enc			
		}
		defer file.Close()
	}

	for _, v := range kv{
		err := fileP[ihash(v.Key) % uint32(nReduce)].Encode(&v)
		if err != nil{
			fmt.Printf("%v\n", err)
		}
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
