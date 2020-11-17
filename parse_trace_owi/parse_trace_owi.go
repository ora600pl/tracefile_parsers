package main

import (
        "log"
	"flag"
	"strconv"
	"os"
	"strings"
	"path/filepath"
	"sync"
	"bufio"
	"regexp"
	"math"
	"fmt"
	"sort"
	"runtime/pprof"
	"time"
	"eventclass"
       )

var DEBUG bool = false
const layout = "2006-01-02 15:04:05.999"
const layout2 = "2006-01-02T15:04:00.999999"

func logme(what string) {
 if DEBUG {
	log.Println(what)
 }
}


func StdDev(x []int64) float64 {
        var sum, mean, sd float64
        for _, elem := range x {
                sum += float64(elem)
        }
        mean = sum / float64(len(x))
        for _, elem := range x {
                sd += math.Pow(float64(elem)-mean, 2)
        }

        sd = math.Sqrt(sd / float64(len(x)))
        return sd
}

//struct for event data
type EventStats struct {
	EventName string
	EventClass string
	Sum float64
	Count int64
	Avg float64
        StdDev float64
	ElaTimes []int64
	Worker int
}

//calculate StdDev for events
func (es *EventStats) CalcStdDev() {
	es.StdDev = StdDev(es.ElaTimes)
}

type MapEvents map[string]*EventStats
type EventStatsSbSum []EventStats
func (a EventStatsSbSum) Len() int           { return len(a) }
func (a EventStatsSbSum) Less(i, j int) bool { return a[j].Sum > a[i].Sum }
func (a EventStatsSbSum) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func showStats(events []EventStats) {
	sort.Sort(EventStatsSbSum(events))
	logme("Events sorted by elapsed time sum")
	fmt.Printf("%60s\t\t%s\t\t\t%s\t\t%s\t\t\t%s\t\t%s\n\n", "WAIT EVENT", "ELA(ms)", "COUNT", "AVG(ms)", "STDDEV(ms)", "CLASS")

	for _, event := range events {
		fmt.Printf("%60s\t\t%f\t\t%d\t\t%f\t\t%f\t\t%s\n", event.EventName, event.Sum/1000, event.Count, event.Avg/1000, event.StdDev/1000, event.EventClass)
	}

}

func parseTrace(traceFile string, workerId int, tF time.Time, tT time.Time) MapEvents {
	tf, ferr := os.Open(traceFile)
	defer tf.Close()
	if ferr != nil {
		log.Panic(ferr)
	}
	r := regexp.MustCompile(`[^\s'=]+|'([^']*)'`)
	rt := regexp.MustCompile(`^\*\*\* [0-9]{4}-([0-9]{2}-?){2}`)
	var eventMap MapEvents
	eventMap = make(map[string]*EventStats)

	scanner := bufio.NewScanner(tf)
	scanner.Split(bufio.ScanLines)
	discoveredTraceDate := false

	for scanner.Scan() {
		traceLine := scanner.Text()
		traceWords := r.FindAllString(traceLine, -1)

		if ti := rt.FindStringIndex(traceLine); ti != nil && !discoveredTraceDate {
			var traceTime time.Time
			if len(traceWords[1]) == 32 {
				traceTime, _ = time.Parse(layout2, traceWords[1][0:26])
			} else {
				traceTime, _ = time.Parse(layout, traceLine[4:])
			}
			//_ = traceTime
			//fmt.Println(traceTime_s)
			//fmt.Println(tFs, tTs, tF, tT)
			if !(traceTime.After(tF) && traceTime.Before(tT)) {
				logme("ignored file: " + traceFile)
				return nil
			}
			discoveredTraceDate = true
		}

		if strings.HasPrefix(traceLine, "WAIT #") {
			eventName := traceWords[3]
			eventEla, _ := strconv.Atoi(traceWords[5])
			eventClassName := eventclass.GetClass(eventName)

			if eventClassName != "Idle" && eventClassName != "Other" {
				if _, ok := eventMap[eventName]; !ok {
					//logme("initializing empty eventMap for the first time")
					eventMap[eventName] = &EventStats{EventName: "",
									  EventClass: "",
									  Sum: 0,
									  Count: 0,
									  Avg: 0,
									  StdDev: 0,
									  Worker: -1,
									  }
				}
				eventMap[eventName].EventName = eventName
				eventMap[eventName].Sum += float64(eventEla)
				eventMap[eventName].Count += 1
				eventMap[eventName].Avg = eventMap[eventName].Sum / float64(eventMap[eventName].Count)
				eventMap[eventName].ElaTimes = append(eventMap[eventName].ElaTimes, int64(eventEla))
				eventMap[eventName].Worker = workerId
				eventMap[eventName].EventClass = eventClassName
			}
		}
	}
	return eventMap
}


//used for profiling CPU
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")


func main() {
	eventclass.InitClassMap()
	tB := time.Now()
	debug := flag.String("d", "false", "debug")
	searchDir := flag.String("s", "", "where to search for trace files")
	parallel := flag.Int("p", 1, "parallel degree")
	timeFrom_s := flag.String("tf", "2020-01-01 00:00:00.100", "time from")
	timeTo_s := flag.String("tt", "2020-01-02 00:00:00.100", "time to")

        flag.Parse()

        tF, _ := time.Parse(layout, *timeFrom_s)
        tT, _ := time.Parse(layout, *timeTo_s)

	if *debug == "true" {
		DEBUG = true
	}

	if *searchDir == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	//used for profiling CPU
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	///////////////////////////


	logme("starting the parse job")

	var traceFiles []string
	root := *searchDir

	//collect information about all trace files in a search directory
        ferr := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
                if err != nil {
                        log.Panic(err)
                }
                if err == nil && !info.IsDir() && strings.HasSuffix(info.Name(), ".trc") {
                        traceFiles = append(traceFiles, path)
                }
                return nil
        })

	if ferr != nil {
		log.Panic(ferr)
	}

	logme("amount of files is: " + strconv.Itoa(len(traceFiles)))
	logme("parallel degree will be: " + strconv.Itoa(*parallel))

	var wg sync.WaitGroup
	tracefileChannel := make(chan string, len(traceFiles))
	logme("created tracefileChannel and now starting to fill it with file names")
	for i:=0; i<len(traceFiles); i++ {
		tracefileChannel <- traceFiles[i]
	}
	close(tracefileChannel)
	logme("chnnel tracefileChannel filled with file names - it will be used for parallel workers")

	waitChannel := make(chan MapEvents, *parallel)
	var me MapEvents
        me = make(map[string]*EventStats)

	for i:=0; i<*parallel; i++ {
		wg.Add(1)
		logme("Starting worker: " + strconv.Itoa(i))
		go func(wid int) {
			t1 := time.Now()
			logme("worker started " + strconv.Itoa(wid));
			defer logme("worker stopped " + strconv.Itoa(wid) + fmt.Sprintf("\t time: %f", time.Now().Sub(t1).Seconds()*1000))
			defer wg.Done()
			for fname := range(tracefileChannel) {
				events := parseTrace(fname, wid, tF, tT)
				if events != nil {
					waitChannel <- events
				}
			}
		} (i)
	}

	go func() {
                logme("Waiting for workers to stop")
                wg.Wait()
                close(waitChannel)
                logme("Workers stopped - waitChannel closed")
        }()

	for events := range(waitChannel) {
		logme("collecting data from channel")
		for _, n := range(events) {
			logme("\tthis was from warker: " + strconv.Itoa(n.Worker))
			if _, ok := me[n.EventName]; !ok {
				me[n.EventName] = n
			} else {
				me[n.EventName].Sum += n.Sum
				me[n.EventName].Count += n.Count
				me[n.EventName].Avg = me[n.EventName].Sum / float64(me[n.EventName].Count)
				me[n.EventName].ElaTimes = append(me[n.EventName].ElaTimes, n.ElaTimes...) //te 3. zeby zlaczyc 2 tablice
			}
			//me[n.EventName].CalcStdDev()
		}

	}

	var events []EventStats
	for _, ev := range(me) {
		ev.CalcStdDev()
		events = append(events, *ev)
	}
	showStats(events)
	logme("Everythong took: " + fmt.Sprintf("%f", time.Now().Sub(tB).Seconds()*1000))
}

