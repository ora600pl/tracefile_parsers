# Parse Trace OWI 
This is a simple tool to parse Oracle SQL trace files for wait events and correlate them with SQLs. 

After downloading the source, you have to set GOPATH to the directory with parse_trace_owi and execute: 
go build parse_trace_owi.go

./parse_trace_owi
  -cpuprofile string
    	write cpu profile to file
  -d string
    	debug (default "false")
  -event string
    	Display SQLids for specified event
  -p int
    	parallel degree (default 1)
  -s string
    	where to search for trace files
  -sqlid string
    	Display wait events for sqlid
  -tf string
    	time from (default "2020-01-01 00:00:00.100")
  -tt string
    	time to (default "2020-01-02 00:00:00.100")
