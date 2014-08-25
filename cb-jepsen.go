package main

import (
       "fmt"
       "encoding/json"
       "flag"
       "log"
       "github.com/couchbaselabs/go-couchbase"
)

type Worker func(string) error

type Report struct {
     done bool // actor is done
     element string // Set element 
     elapsed int64 // elapsed time to store the record
     err error // Possible error
     ack bool // if the element was acknowledge
}

type Options struct {
     url string
     bucket string
     key string
     actors int64
     setsize int64
}

func parseOptions() Options {
     var o Options
     flag.StringVar(&o.url, "url", "http://127.0.0.1:8091/", "URL to Couchbase server")
     flag.StringVar(&o.bucket, "bucket", "default", "Bucket to store the objects into")
     flag.StringVar(&o.key, "key", "jepsen", "The key to use for the set")
     flag.Int64Var(&o.actors, "actors", 5, "Number of concurrent actors to use")
     flag.Int64Var(&o.setsize, "setsize", 2000, "Number of items in the set")
     flag.Parse()
     return o;
}


func printReportLn(report Report) {
    if !report.done {
        log.Printf("%s %t %v", report.element, report.ack, report.err)
    }
}

func getBucket(o Options) (*couchbase.Bucket, error) {
     return couchbase.GetBucket(o.url, "default", o.bucket)
}

func actor(o Options, bucket *couchbase.Bucket, out chan Report, actorID int64) {
     var i int64
     var err error
     hashSet := make(map[string]bool)
     
     for i = actorID; i < o.setsize; i += o.actors {
         element := fmt.Sprintf("%d", i)
         // update the hashset
	 err = bucket.Update(o.key, 0, func(current []byte) ([]byte, error) {
             json.Unmarshal(current, &hashSet)
	     hashSet[element] = true
	     return json.Marshal(&hashSet)
	 })
	 out <- Report{false, element, -1, err, err == nil}
     }
     out <- Report{true, "", -1, nil, false}      
}


func runActors(o Options, bucket *couchbase.Bucket) {
     var err error
     var i int64
     reportSet := make(map[string]Report)
     hashSet := make(map[string]bool)

     // the output channel
     out := make(chan Report, 1000 * o.actors)
     runningActors := 0

     // Initalize the value
     err = bucket.Set(o.key, 0, &hashSet)
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

     err = bucket.Get(o.key, &hashSet)
     if err != nil {
         log.Fatalf("Error getting the remote set: %v", err)
     }

     analyzeSet(o, reportSet, hashSet)
}

func analyzeSet(o Options, reportSet map[string]Report, hashSet map[string]bool) {
     var acknowledged int64
     var ackAndFound int64
     var ackButLost int64
     var foundButNotAck int64
     var completelyLost int64
     var i int64

     for i = 0; i < o.setsize; i++ {
        // was the element acked?
	element := fmt.Sprintf("%d", i)
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
     log.Printf("%d total", o.setsize)
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
     runActors(o, bucket)
}

