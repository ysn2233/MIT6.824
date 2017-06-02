package mapreduce

import (
	"log"
	"os"
	"encoding/json"
	"sort"
)
// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// initialize a map to store the values of each key
	kvpairs := make(map[string] []string)
	// read the intermediate file, decode them and then put the keys and values into a map
	for i:=0; i<nMap; i++ {
		filename := reduceName(jobName, i, reduceTaskNumber)
		file, err := os.Open(filename)
		defer file.Close()
		if (err != nil) {
			log.Fatal("doReduce: map file open error!")
		}
		var kvpair KeyValue
		dec := json.NewDecoder(file)
		for  {
			err = dec.Decode(&kvpair)
			if (err != nil) {
				break
			}
			_, ok := kvpairs[kvpair.Key]
			if !ok {
				kvpairs[kvpair.Key] = make([]string, 0)
			}
			kvpairs[kvpair.Key] = append(kvpairs[kvpair.Key], kvpair.Value)
		}
	}


	// output the result in the order of key.
	var keys []string
	for key, _ := range kvpairs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	file, err := os.Create(outFile)
	if (err != nil) {
		log.Fatal("doReduce: outfile open error!")
	}
	enc := json.NewEncoder(file)
	for key := range kvpairs {
		err := enc.Encode(&KeyValue{key, reduceF(key, kvpairs[key])})
		if (err != nil) {
			log.Fatal("doReduce: json encode error!")
		}
	}
	file.Close()
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
}
