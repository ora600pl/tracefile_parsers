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
	"encoding/gob"
	"bytes"
       )

var DEBUG bool = false
const layout = "2006-01-02 15:04:05.999"
const layout2 = "2006-01-02T15:04:05.999999"

func logme(what string) {
 if DEBUG {
	log.Println(what)
 }
}

func fileExists(filename string) bool {
    info, err := os.Stat(filename)
    if os.IsNotExist(err) {
        return false
    }
    return !info.IsDir()
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

type SQLEventStats struct {
	Sum float64
	Count int64
	ElaTimes []int64
	PossibleOperations []string
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
	SQLtimes map[string]*SQLEventStats
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

type SQLStat struct {
	SQLid string
	Ela float64
}
type SQLStats []SQLStat
func (a SQLStats) Len() int           { return len(a) }
func (a SQLStats) Less(i, j int) bool { return a[j].Ela > a[i].Ela }
func (a SQLStats) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func removeDupString(strSlice []string) []string {
	keys := make(map[string]bool)
	list := []string{}
	for _, entry := range strSlice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
    }
    return list
}

func showStats(events []EventStats, topN *int) {
	sort.Sort(EventStatsSbSum(events))
	logme("Events sorted by elapsed time sum")
	fmt.Printf("%60s\t\t%s\t\t\t%s\t\t%s\t\t\t%s\t\t%s\n\n", "WAIT EVENT", "ELA(ms)", "COUNT", "AVG(ms)", "STDDEV(ms)", "CLASS")
	listLen := len(events)
	limit := -1
	if *topN > 0 {
		limit = listLen - *topN - 1
	}
	for i, event := range events {
		if i > limit {
			event.CalcStdDev()
			fmt.Printf("%60s\t\t%f\t\t%d\t\t%f\t\t%f\t\t%s\n", event.EventName, event.Sum/1000, event.Count, event.Avg/1000, event.StdDev/1000, event.EventClass)
		}
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

	cursorToSQLid := make(map[string]string)
	type cursorObjKey struct {
		objId string
		cursorId string
	}
	cursorObjToOp := make(map[cursorObjKey][]string)

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
			if !(traceTime.After(tF) && traceTime.Before(tT)) {
				logme("ignored file: " + traceFile + " with traceTime = " + traceTime.String() + " with params " + tF.String() + " " + tT.String())
				return nil
			}
			discoveredTraceDate = true
		}

		if strings.HasPrefix(traceLine, "WAIT #") {
			eventName := traceWords[3]
			eventEla, _ := strconv.Atoi(traceWords[5])
			eventClassName := GetClass(eventName)

			if eventClassName != "Idle" && eventClassName != "Other" {
				if _, ok := eventMap[eventName]; !ok {
					eventMap[eventName] = &EventStats{EventName: "",
									  EventClass: "",
									  Sum: 0,
									  Count: 0,
									  Avg: 0,
									  StdDev: 0,
									  Worker: -1,
									  SQLtimes: make(map[string]*SQLEventStats),
									  }
				}
				eventMap[eventName].EventName = eventName
				eventMap[eventName].Sum += float64(eventEla)
				eventMap[eventName].Count += 1
				eventMap[eventName].Avg = eventMap[eventName].Sum / float64(eventMap[eventName].Count)
				eventMap[eventName].ElaTimes = append(eventMap[eventName].ElaTimes, int64(eventEla))
				eventMap[eventName].Worker = workerId
				eventMap[eventName].EventClass = eventClassName

				cursor_id := traceWords[1][0:len(traceWords[1])-1]
				if _, ok := cursorToSQLid[cursor_id]; ok {
					if _, ok2 := eventMap[eventName].SQLtimes[cursorToSQLid[cursor_id]]; !ok2 {
						eventMap[eventName].SQLtimes[cursorToSQLid[cursor_id]] = &SQLEventStats{Sum: 0, Count: 0}
					}
					eventMap[eventName].SQLtimes[cursorToSQLid[cursor_id]].Sum += float64(eventEla)
					eventMap[eventName].SQLtimes[cursorToSQLid[cursor_id]].Count += 1
					eventMap[eventName].SQLtimes[cursorToSQLid[cursor_id]].ElaTimes = append(eventMap[eventName].SQLtimes[cursorToSQLid[cursor_id]].ElaTimes, int64(eventEla))
					logme("SQLID for cursor: " + cursor_id + " " + cursorToSQLid[cursor_id] + " " + eventName + " " + traceFile)
					if strings.Index(traceLine, "obj#=") > 0 {
						objId := traceWords[len(traceWords)-3]
						logme("Looking for operation map for objd: " + objId + " sqlid: " + cursorToSQLid[cursor_id])
						if v, ok3 := cursorObjToOp[cursorObjKey{objId, cursor_id}]; ok3 {
							logme("Found: " + v[0])
							eventMap[eventName].SQLtimes[cursorToSQLid[cursor_id]].PossibleOperations = append(eventMap[eventName].SQLtimes[cursorToSQLid[cursor_id]].PossibleOperations, v...)
						} else {
							logme("Not found - creating a placeholder to fill later")
							eventMap[eventName].SQLtimes[cursorToSQLid[cursor_id]].PossibleOperations = append(eventMap[eventName].SQLtimes[cursorToSQLid[cursor_id]].PossibleOperations, objId + " " + cursor_id)
						}
					}
				}
			}
		} else if strings.HasPrefix(traceLine, "PARSING IN CURSOR") {
			cursor_id := traceWords[3]
			sql_id := traceWords[len(traceWords)-1]
			logme("SQLID for cursor: " + cursor_id + " " + sql_id)
			cursorToSQLid[cursor_id] = sql_id
		} else if strings.HasPrefix(traceLine, "STAT #") {
			cursor_id := traceWords[1]
			objId :=traceWords[11]
			foundElem := false
			for _, opname := range(cursorObjToOp[cursorObjKey{objId, cursor_id}]) {
				if opname == traceWords[len(traceWords)-1] {
					foundElem = true
				}
			}
			if !foundElem {
				cursorObjToOp[cursorObjKey{objId, cursor_id}] = append(cursorObjToOp[cursorObjKey{objId, cursor_id}], traceWords[len(traceWords)-1])
			}
			logme("OP for cursor: " + cursor_id + " obj: " + objId + " op: " + traceWords[len(traceWords)-1] + " sqlid: " + cursorToSQLid[cursor_id])
			for _, ev := range(eventMap) {
				if sqlstat, sqlidOk := ev.SQLtimes[cursorToSQLid[cursor_id]]; sqlidOk {
					for i, op := range(sqlstat.PossibleOperations) {
						if objId + " " + cursor_id == op {
							if len(cursorObjToOp[cursorObjKey{objId, cursor_id}]) == 1 {
								sqlstat.PossibleOperations[i] = cursorObjToOp[cursorObjKey{objId, cursor_id}][0]
							} else if len(cursorObjToOp[cursorObjKey{objId, cursor_id}]) > 1 {
								sqlstat.PossibleOperations[i] = cursorObjToOp[cursorObjKey{objId, cursor_id}][0]
								sqlstat.PossibleOperations = append(sqlstat.PossibleOperations, cursorObjToOp[cursorObjKey{objId, cursor_id}][1:]...)
							}
						}
					}
				}
			}
		}
	}
	return eventMap
}


//used for profiling CPU
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")


func main() {
	InitClassMap()
	tB := time.Now()
	debug := flag.String("d", "false", "debug")
	searchDir := flag.String("s", "", "where to search for trace files")
	parallel := flag.Int("p", 1, "parallel degree")
	timeFrom_s := flag.String("tf", "2020-01-01 00:00:00.100", "time from")
	timeTo_s := flag.String("tt", "2020-01-02 00:00:00.100", "time to")
	eventName := flag.String("event", "", "Display SQLids for specified event")
	sqlId := flag.String("sqlid","", "Display wait events for sqlid")
	topN := flag.Int("top", 0, "Only TOP n elements")

        flag.Parse()

        tF, _ := time.Parse(layout, *timeFrom_s)
        tT, _ := time.Parse(layout, *timeTo_s)

	saveFileName := *timeFrom_s + "_" + *timeTo_s
	saveFileName = strings.ReplaceAll(saveFileName, ":", "")
	saveFileName = strings.ReplaceAll(saveFileName, " ", "")
	saveFileName = strings.ReplaceAll(saveFileName, "-", "")
	saveFileName = strings.ReplaceAll(saveFileName, ".", "")
	saveFileName = saveFileName + ".owisave"

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

	var me MapEvents
        me = make(map[string]*EventStats)

	if !fileExists(saveFileName) {
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
		logme("chnnel tracefileChannel filled with file names - it will be used for parallel workers, a Rafal ma malego..")

		waitChannel := make(chan MapEvents, *parallel)

		for i:=0; i<*parallel; i++ {
			wg.Add(1)
			logme("Starting worker: " + strconv.Itoa(i))
			go func(wid int) {
				t1 := time.Now()
				logme("worker started " + strconv.Itoa(wid));
				defer wg.Done()
				for fname := range(tracefileChannel) {
					events := parseTrace(fname, wid, tF, tT)
					if events != nil {
						waitChannel <- events
					}
				}
				logme("worker stopped " + strconv.Itoa(wid) + fmt.Sprintf("\t time: %f", float64(time.Now().Sub(t1).Nanoseconds()/1000/1000)))
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
					for sqlid, sqlstat := range(n.SQLtimes) {
						if _, ok := me[n.EventName].SQLtimes[sqlid]; !ok {
							me[n.EventName].SQLtimes[sqlid] = sqlstat
						} else {
							me[n.EventName].SQLtimes[sqlid].Sum += sqlstat.Sum
							me[n.EventName].SQLtimes[sqlid].Count += sqlstat.Count
							me[n.EventName].SQLtimes[sqlid].ElaTimes = append(me[n.EventName].SQLtimes[sqlid].ElaTimes, sqlstat.ElaTimes...)
							me[n.EventName].SQLtimes[sqlid].PossibleOperations = append(me[n.EventName].SQLtimes[sqlid].PossibleOperations, sqlstat.PossibleOperations...)
						}
						me[n.EventName].SQLtimes[sqlid].PossibleOperations = removeDupString(me[n.EventName].SQLtimes[sqlid].PossibleOperations)
					}
				}
				//me[n.EventName].CalcStdDev()
			}

		}


		//remember the structure map
		var eventBin bytes.Buffer
		enc := gob.NewEncoder(&eventBin)
		err := enc.Encode(me)
		if err != nil {
			log.Panic(err)
		}

		eventBinF, errf := os.Create(saveFileName)
		if errf != nil {
			log.Panic(errf)
		}

		eventBin.WriteTo(eventBinF)
		eventBinF.Close()
		/////////////////////////////////
	} else { //and read the structure map
		var eventBin bytes.Buffer
		dec := gob.NewDecoder(&eventBin)
		eventBinF, errf := os.Open(saveFileName)
		if errf != nil {
			log.Panic(errf)
		}

		eventBin.ReadFrom(eventBinF)
		dec.Decode(&me)
	}

	sqlIDsEla := make(map[string]float64)
	for _, es := range(me) {
		for sqlid, ela := range(es.SQLtimes) {
			if _, ok := sqlIDsEla[sqlid]; !ok {
				sqlIDsEla[sqlid] = ela.Sum
			} else {
				sqlIDsEla[sqlid] += ela.Sum
			}
		}
	}

	if *eventName != "" && *sqlId == "" {
		sqlEla := float64(0)
		fmt.Printf("SQLs for event %s\n", *eventName)
		var sqlTimes SQLStats
		sqlCnt := uint64(0)
		for sqlid, ela := range(me[*eventName].SQLtimes) {
			sqlTimes = append(sqlTimes, SQLStat{SQLid: sqlid, Ela: ela.Sum})
			sqlEla += ela.Sum
		}
		sort.Sort(sqlTimes)
		listLen := len(sqlTimes)
		limit := -1
		if *topN > 0 {
			limit = listLen - *topN - 1
		}
		for i, s := range(sqlTimes) {
			if i > limit {
				fmt.Printf("%s\t\t%f\n", s.SQLid, s.Ela/1000)
			}
			sqlCnt += 1
		}
		fmt.Printf("It was %f percent out of all SQLs - %d out of %d\n", float64(sqlCnt)/float64(len(sqlIDsEla))*100, sqlCnt, len(sqlIDsEla))

	} else if *sqlId != "" && *eventName == "" {
		fmt.Println("Wait events for this SQLid")
		var events []EventStats
		for _, eventStat := range(me) {
			if stat, ok := eventStat.SQLtimes[*sqlId]; ok {
				eventStat.Sum = stat.Sum
				eventStat.Count = stat.Count
				eventStat.Avg = stat.Sum / float64(stat.Count)
				eventStat.ElaTimes = stat.ElaTimes
				events = append(events, *eventStat)
			}
		}
		showStats(events, topN)
	} else if *sqlId != "" && *eventName != "" {
		fmt.Println("Possible operations for wait event " + *eventName + " and SQLID " + *sqlId)
		for _, v := range(me[*eventName].SQLtimes[*sqlId].PossibleOperations) {
			fmt.Println(v)
		}
	} else {
		var events []EventStats
                for _, ev := range(me) {
                        events = append(events, *ev)
                }
		showStats(events, topN)
	}
	fmt.Println("Everythong took: " + fmt.Sprintf("%f", time.Now().Sub(tB).Seconds()*1000) + " ms")
}

