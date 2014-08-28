package main

import (
       "fmt"
       "encoding/json"
       "flag"
       "log"
       "time"
       "github.com/couchbaselabs/go-couchbase"
)

type Worker func() (bool, error)

type Report struct {
     done bool // actor is done
     element string // Set element 
     timestamp int64 // timestamp
     elapsed int64 // elapsed time to store the record
     ack bool // if the element was acknowledge
     err error // Possible error
}

type Options struct {
     url string
     bucket string
     key string
     actors int64
     setsize int64
     mode string
     persist bool
}

func parseOptions() Options {
     var o Options
     flag.StringVar(&o.url, "url", "http://127.0.0.1:8091/", "URL to Couchbase server")
     flag.StringVar(&o.bucket, "bucket", "default", "Bucket to store the objects into")
     flag.StringVar(&o.key, "key", "jepsen", "The key to use for the set")
     flag.Int64Var(&o.actors, "actors", 5, "Number of concurrent actors to use")
     flag.Int64Var(&o.setsize, "setsize", 2000, "Number of items in the set")
     flag.StringVar(&o.mode, "mode", "cas", "incr | cas | naive; naive is without CAS")
     flag.BoolVar(&o.persist, "persist", false, "wait for persistance")
     flag.Parse()
     if o.mode != "cas" && o.mode != "naive" && o.mode != "incr" {
        log.Fatalf("mode can only be \"incr\", \"cas\" or \"naive\", you gave %s", o.mode)
     }
     return o;
}


func printHeader() {
     fmt.Printf("element, ack, elapsed, timestamp, error\n")
}

func printReportLn(report Report) {
    var errorStr string
    var ackInt int    
    if report.err == nil {
        errorStr = ""
    } else {
        errorStr = fmt.Sprintf("%v", report.err)
    }
    if report.ack {
       ackInt = 1
    } else {
       ackInt = 0
    } 
    if !report.done {
        fmt.Printf("\"%s\", %d, %d, %d, \"%s\"\n", report.element, ackInt, report.elapsed, report.timestamp, errorStr, )
    }
}

func getBucket(o Options) (*couchbase.Bucket, error) {
     return couchbase.GetBucket(o.url, "default", o.bucket)
}


func doWork(element string, worker Worker) Report {
 start := time.Now().UnixNano()
 ack, err := worker()
 end := time.Now().UnixNano()
 return Report{false, element, end, end-start, ack, err}
}


func actor(o Options, bucket *couchbase.Bucket, out chan Report, actorID int64) {
     var i int64
     var err error
     var writeOptions couchbase.WriteOptions

     hashSet := make(map[string]bool)
     if o.persist {
        writeOptions = couchbase.Persist
     } else {
	writeOptions = 0
     }

     for i = actorID; i < o.setsize; i += o.actors {
         element := fmt.Sprintf("%d", i)

	 incrWork := func() (bool, error) {
	    _, err := bucket.Incr(o.key, 1, 0, 0)
	    return err == nil, err
	 }	 

	 casWork := func() (bool, error) {
             err = bucket.WriteUpdate(o.key, 0, func(current []byte) ([]byte, couchbase.WriteOptions, error) {
	         var updated []byte
                 json.Unmarshal(current, &hashSet)
	         hashSet[element] = true
	         updated, err = json.Marshal(&hashSet)
		 return updated, writeOptions, err
	     })
	     return err == nil, err
     	 }

         naiveWork := func()(bool, error) {
             err = bucket.Get(o.key, &hashSet)
	     if err != nil {
	         return false, err
	     }
	     hashSet[element] = true
	     err = bucket.Write(o.key, 0, 0, &hashSet, writeOptions)
	     return err == nil, err
     	 }

         // update the hashset
	 if o.mode == "incr" {
	     out <- doWork(element, incrWork)
	 } else if o.mode == "naive" {
             out <- doWork(element, naiveWork)
	 } else {
             out <- doWork(element, casWork)
         }
     }
     out <- Report{true, "", -1, -1, false, nil}      
}


func runActors(o Options, bucket *couchbase.Bucket) {
     var err error
     var i int64
     var counter int64
     reportSet := make(map[string]Report)
     hashSet := make(map[string]bool)

     // the output channel
     out := make(chan Report, 1000 * o.actors)
     runningActors := 0

     // Initalize the value
     if o.mode == "incr" {
          err = bucket.Set(o.key, 0, 0)
     } else {
          err = bucket.Set(o.key, 0, &hashSet)
     }    

     if err != nil {
       log.Fatalf("Unable to initialize the value: %v", err)
     }

     // Run the actors
     for i = 0; i < o.actors; i++ {
          log.Printf("Starting actor %d\n", i)
          go actor(o, bucket, out, i)
	  runningActors++
     }

     // Receive the reports from the actors
     printHeader()
     for {
        report := <-out

	if !report.done {
	    reportSet[report.element] = report
	    printReportLn(report)
	} else {
	   runningActors--
           if runningActors == 0 {
	     break
           }
       }
     }


     if o.mode == "incr" {
        err = bucket.Get(o.key, &counter)
        if err != nil {
           log.Fatalf("Error getting the remote set: %v", err)
        }
        analyzeCount(o, reportSet, counter)
     } else {
        start := time.Now()

        for {
            err = bucket.Get(o.key, &hashSet)
            if err != nil {
	        log.Printf("Error getting the remote set: %v, retrying",
		    err)
            } else {
	        log.Printf("Retrieved remote set after %s seconds",
		  time.Since(start) * time.Second)
	        break
	    }
	}    
	
        analyzeSet(o, reportSet, hashSet)
    }
}

func analyzeCount(o Options, reportSet map[string]Report, count int64) {
     var acknowledged int64
     var total int64
     var i int64

     for i = 0; i < o.setsize; i++ {
        // was the element acked?
	element := fmt.Sprintf("%d", i)
	if _, ok := reportSet[element]; ok {
	   total++
	}
	if reportSet[element].ack {
            acknowledged++
        }	
     }
          
     log.Printf("%d total", total)
     log.Printf("%d total loss", total - count)
     log.Printf("%d acknowledged", acknowledged)
}

func analyzeSet(o Options, reportSet map[string]Report, hashSet map[string]bool) {
     var total int64
     var acknowledged int64
     var ackAndFound int64
     var ackButLost int64
     var foundButNotAck int64
     var completelyLost int64
     var i int64

     for i = 0; i < o.setsize; i++ {
        // was the element acked?
	element := fmt.Sprintf("%d", i)
	if _, ok := reportSet[element]; ok {
	    total++
	}

	if reportSet[element].ack {
	    acknowledged++

	    if hashSet[element] {
	       ackAndFound++
	    } else {
	       ackButLost++
	    }
	} else if hashSet[element] {
	   foundButNotAck++
	} else {
	   completelyLost++
	}
     }
     log.Printf("%d total", total)
     log.Printf("%d total loss", completelyLost + ackButLost)

     log.Printf("%d acknowledged", acknowledged)
     log.Printf("%d acknowledged and found", ackAndFound)
     log.Printf("%d acknowledged but lost", ackButLost)
     log.Printf("%d found but not acknowledged", foundButNotAck)
     log.Printf("%d lost without acknowledgement", completelyLost)

}

func main() {
     o := parseOptions()
     bucket, err := getBucket(o)

     if err != nil {
       log.Fatalf("Error getting bucket: %v", err)
     }
     log.Printf("Couchbase Nodes: %v", bucket.NodeAddresses())
     vbucket := bucket.VBHash(o.key)
     serverMap := bucket.VBServerMap()
     node_ids := serverMap.VBucketMap[vbucket]
     replicas := make([]string, len(node_ids))
     for id := range node_ids {
        replicas[id] = serverMap.ServerList[id]
     }
     log.Printf("Nodes for '%s': %v", o.key, replicas)
     runActors(o, bucket)
}

