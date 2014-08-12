package main

import (
       "runtime"
       "fmt"
       "flag"
       "log"
       "time"
       "strconv"
       "github.com/couchbaselabs/go-couchbase"
)

type Report struct {
     done bool
     elapsedMS int64
     err error
     timestamp int64
}

type Options struct {
     url string
     bucket string
     objectSize int64
     iterations int64
     poolsize int
     actors int
}

func makeObjectVal(objectSize int64) []byte {
     data := make([]byte, objectSize)     
     for index, _ := range data {
        data[index] = 'a';
     }
     return data;
}
    

func actor(jobStart int64, objectVal []byte, bucket *couchbase.Bucket, o Options, out chan Report) {
    var i int64
    var err error
    for i = 0; i < o.iterations; i++ {
        key := strconv.FormatInt(i, 10)
        start := time.Now().UnixNano()

	// do work
	err = bucket.SetRaw(key, 0, objectVal)
	if err == nil {
	   _, err = bucket.GetRaw(key)
	}
	end := time.Now().UnixNano()
        out <- Report{false, end-start, err, end-jobStart}
    }
    out <- Report{true, -1, nil, -1}
}

func parseOptions() Options {
     var o Options
     flag.StringVar(&o.url, "url", "http://127.0.0.1:8091/", "URL to Couchbase server")
     flag.StringVar(&o.bucket, "bucket", "default", "Bucket to store the objects into")
     flag.Int64Var(&o.objectSize, "size", 1000, "Size of the object to store")
     flag.Int64Var(&o.iterations, "iterations", 100000, "Size of the object to store")
     flag.IntVar(&o.actors, "actors", 1, "Size of the object to store")
     flag.IntVar(&o.poolsize, "poolsize", 4, "Size of the object to store")
     flag.Parse()
     return o;
}

func printHeader() {
     fmt.Printf("elapsed, error, timestamp\n");
}

func printReportLn(report Report) {
    if !report.done {
      fmt.Printf("%d, ", report.elapsedMS)

      if report.err != nil {
         fmt.Printf("%v,", report.err)
      } else {
         fmt.Printf(",")
      }
      fmt.Printf("%d\n", report.timestamp)
    }
}

func main() {
     log.Printf("Using %d\n CPUs", runtime.NumCPU())
     runtime.GOMAXPROCS(runtime.NumCPU())
     o := parseOptions()
     couchbase.PoolSize = o.poolsize

     c, err := couchbase.Connect(o.url)
     if err != nil {
       log.Fatalf("Error connecting: %v", err)
     }

     pool, err := c.GetPool("default")
     if err != nil {
       log.Fatalf("Error getting pool: %v", err)
     }

     bucket, err := pool.GetBucket(o.bucket)
     if err != nil {
       log.Fatalf("Error getting bucket: %v", err)
     }

     log.Printf("Couchbase Nodes: %v", bucket.NodeAddresses())

     objectVal := makeObjectVal(o.objectSize)

     // the output channel
     out := make(chan Report, 1000 * o.actors) 
     runningActors := 0

     for i := 0; i < o.actors; i++ {
          log.Printf("Starting actor %d\n", i)
          go actor(time.Now().UnixNano(), objectVal, bucket, o, out)
	  runningActors++
     }  

     printHeader()
     for {
        report := <-out
	printReportLn(report)
	if report.done {
	   runningActors--
        }
	if runningActors == 0 {
	   break
        }
     }
}
