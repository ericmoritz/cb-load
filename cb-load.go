package main

import (
       "runtime"
       "math/rand"
       "fmt"
       "flag"
       "log"
       "time"
       "github.com/couchbaselabs/go-couchbase"
)

type Worker func(string) error

type Report struct {
     done bool
     elapsedMS int64
     err error
     timestamp int64
     op string
     key string
}

type Options struct {
     url string
     bucket string
     objectSize int64
     keyspace int64
     poolsize int
     actors int64
     duration int64
     readCount int64
     writeCount int64
     incrCount int64
     casCount int64
     quiet bool
     fillBucket bool
     persist bool
}

func makeObjectVal(objectSize int64) []byte {
     data := make([]byte, objectSize)     
     for index, _ := range data {
        data[index] = 'a';
     }
     return data;
}
    
func doAction(i int64, every int64) bool {
     return every > 0 && i % every == 0
}

func doWork(kind string, key string, worker Worker) Report {
 start := time.Now().UnixNano()
 err := worker(key)
 end := time.Now().UnixNano()
 return Report{false, end-start, err, end, kind, key}
}

func sendReport(o Options, out chan Report, report Report) {
   if !o.quiet {
     out <- report;
   }
}

func doWorkAndSendReport(o Options, out chan Report, kind string, opCount int64, worker Worker) {
    var i int64
    for i = 0; i < opCount; i++ {
      key := generateRandomKey(o, kind)
      sendReport(o, out, doWork(kind, key, worker))
    }
}

func generateRandomKey(o Options, kind string) string {
     return generateKey(kind, rand.Int63n(o.keyspace))
}

func generateKey(kind string, i int64) string {
     if kind == "write" || kind == "read" {
         kind = "readWrite"
     }
     return fmt.Sprintf("%s:%d", kind, i)
}

func durationElapsed(duration int64, jobStart time.Time, now time.Time) bool {
     elapsed := now.Unix() - jobStart.Unix()
     return elapsed > duration
} 

func fillBucket(o Options, bucket *couchbase.Bucket, objectVal []byte) {
     var i int64
     for i = 0; i < o.keyspace; i++ {
         err := bucket.SetRaw(generateKey("write", i), 0, objectVal)
	 if err != nil { log.Fatal(err) }
     }
}


func actor(jobStart time.Time, objectVal []byte, o Options, bucket *couchbase.Bucket, out chan Report) {
     var writeOptions couchbase.WriteOptions
     if o.persist {
         writeOptions = couchbase.Persist
     } else {
         writeOptions = 0
     }

    for ; !durationElapsed(o.duration, jobStart, time.Now()) ; {
      doWorkAndSendReport(o, out, "read", o.readCount, func(key string) error {
          _, err := bucket.GetRaw(key)
	  return err
      })
      doWorkAndSendReport(o, out, "write", o.writeCount, func(key string) error {
          return bucket.Write(key, 0, 0, objectVal, writeOptions | couchbase.Raw)
      })
      doWorkAndSendReport(o, out, "incr", o.incrCount, func(key string) error {
          _, err := bucket.Incr(key, 1, 0, 0)
	  return err
      })
      doWorkAndSendReport(o, out, "cas", o.casCount, func(key string) error {
          return bucket.WriteUpdate(key, 0, func(current []byte) ([]byte, couchbase.WriteOptions, error) {
	      objectVal[0] = objectVal[0] + 1
	      return objectVal, writeOptions, nil
	  })
      })

    }
    out <- Report{true, -1, nil, -1, "", ""} 
}

func parseOptions() Options {
     var o Options
     flag.StringVar(&o.url, "url", "http://127.0.0.1:8091/", "URL to Couchbase server")
     flag.StringVar(&o.bucket, "bucket", "default", "Bucket to store the objects into")
     flag.Int64Var(&o.objectSize, "size", 1000, "Size of the object to store")
     flag.Int64Var(&o.keyspace, "keyspace", 10000, "Number of unique keys")
     flag.Int64Var(&o.actors, "actors", 1, "Number of concurrent actors to use")
     flag.IntVar(&o.poolsize, "poolsize", 4, "Connection pool size")
     flag.Int64Var(&o.duration, "duration", 300, "duration in seconds (zero to run forever)")
     flag.Int64Var(&o.readCount, "readCount", 0, "number of reads to do each iteration")
     flag.Int64Var(&o.writeCount, "writeCount", 0, "number of writes to do each iteration")
     flag.Int64Var(&o.incrCount, "incrCount", 0, "number of increments to do each iteration")
     flag.Int64Var(&o.casCount, "casCount", 0, "number of CAS operations to do each iteration")
     flag.BoolVar(&o.quiet, "quiet",  false, "turn off logging for performance")
     flag.BoolVar(&o.fillBucket, "fillBucket",  false, "fill the bucket before running operations")
     flag.BoolVar(&o.persist, "persist",  false, "wait for persistance")
     flag.Parse()
     return o;
}

func printHeader() {
     fmt.Printf("elapsed, error, timestamp, type, key\n");
}

func printReportLn(report Report) {
    if !report.done {
      fmt.Printf("%d, ", report.elapsedMS)

      if report.err != nil {
         fmt.Printf("\"%v\", ", report.err)
      } else {
         fmt.Printf(",")
      }
      fmt.Printf("%d, %s, %s\n", report.timestamp, report.op, report.key)
    }
}

func getBucket(o Options) (*couchbase.Bucket, error) {
     return couchbase.GetBucket(o.url, "default", o.bucket)
}

func runActors(o Options, objectVal []byte, bucket *couchbase.Bucket) {
     var i int64
     // the output channel
     out := make(chan Report, 1000 * o.actors)
     runningActors := 0

     for i = 0; i < o.actors; i++ {
          log.Printf("Starting actor %d\n", i)
          go actor(time.Now(), objectVal, o, bucket, out)
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

func main() {
     log.Printf("Using %d\n CPUs", runtime.NumCPU())
     runtime.GOMAXPROCS(runtime.NumCPU())
     o := parseOptions()
     couchbase.PoolSize = o.poolsize
     bucket, err := getBucket(o)
     if err != nil {
       log.Fatalf("Error getting bucket: %v", err)
     }
     log.Printf("Couchbase Nodes: %v", bucket.NodeAddresses())

     objectVal := makeObjectVal(o.objectSize)
     if o.fillBucket {
        log.Printf("Filling Bucket...")
        fillBucket(o, bucket, objectVal);
        log.Printf("Bucket Full...")
     }
     runActors(o, objectVal, bucket)
}

