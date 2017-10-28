# Define some starting global variables
BEGIN {
	# Number of the cursor for WAIT# from 10046 trace file
	cursor_no="start sequence nomatch";
	# Information about closing the cursor in 10046 trace file
	cursor_no_close="start sequence nomatch";
	# SQL_ID will found based on 10053 header section
	sql_id="start sql id";
	rewinded=0;
}

# The 10053 header section - determining SQL_ID and printing orignal SQL before transformations - then rewinding the file from the begining to find 10046 info.
/OPTIMIZER INFORMATION/ {
	getline nextline;
	getline nextline;
	getline nextline;
	sql_id=substr(nextline,index(nextline,"sql_id="),20);
	getline nextline;
	print "\nORIGINAL SQL:";
	while(nextline !~ "Legend")
	{
		print nextline;
		getline nextline;
	}
  }

# Printing final query after transformation with some basic formating for easier reading
/Final query after transformations:/{
	getline nextline;
	print sql_id;
	sql_text=nextline;
	gsub(/,/,"\n\t,",sql_text);
	gsub(/FROM/,"\n\tFROM",sql_text);
	gsub(/WHERE/,"\n\t\tWHERE",sql_text);
	gsub(/GROUP BY/,"\n\t\tGROUP BY",sql_text);
	gsub(/HAVING/,"\n\t\tHAVING",sql_text);
	gsub(/ AND /,"\n\t\t AND ",sql_text);
	gsub(/ OR /,"\n\t\t OR ",sql_text);
	print sql_text;
	print " ";
}

# Printing Plan Table output and outline data from 10053 trace file
/Explain Plan Dump/{
	getline nextline;
	while(nextline !~ "Optimizer state dump")
	{
		print nextline;
		getline nextline;
	}
}

# Determining the cursor number in 10046 trace file to check wait events
/PARSING IN CURSOR/ {
	if($0 ~ substr(sql_id,8))
	{
		cursor_no="WAIT " $4;
		cursor_no_close="CLOSE " $4;
	}
	else if($0 ~ "dep=0" && $0 ~ "oct=3")
	{
		cursor_no="WAIT " $4;
		cursor_no_close="CLOSE " $4;
		sql_id=substr($NF,8,13);
	}
}
{
# Filter out *Net* wait events and create histogram in ms - summerize waiting times 
  if($0 ~ cursor_no && $0 !~ "Net") {
	event_part=substr($0,index($0,"'")+1); 
	event_name=substr(event_part,1,index(event_part,"'")-1); 
	event_time1=substr($0,index($0,"ela= ")+5);
	event_time=substr(event_time1,1,index(event_time1," ")-1);
	events[event_name]+=event_time;
	event_cnt[event_name]++;


	event_time_m=event_time/1000;
	if(event_time_m<=1)
	{
		event_hist[event_name,"<= 1"]++;
	}
	else if(event_time_m>1 && event_time_m<=2)
	{
		event_hist[event_name,"<= 2"]++;
	}
	else if(event_time_m>2 && event_time_m<=4)
        {
                event_hist[event_name,"<= 4"]++;
        }
	else if(event_time_m>4 && event_time_m<=8)
        {
                event_hist[event_name,"<= 8"]++;
        }
	else if(event_time_m>8 && event_time_m<=16)
        {
                event_hist[event_name,"<=16"]++;
        }
	else if(event_time_m>16 && event_time_m<=32)
        {
                event_hist[event_name,"<=32"]++;
        }
	else if(event_time_m>32)
        {
                event_hist[event_name,">32"]++;
        }

	if($0 ~ "obj#")
	{
		object_wait[$(NF-1)]+=event_time;	
	}
  }
}
# Printing wait event info at cursor close
{
  if($0 ~ cursor_no_close) {
	br=0;
	n=asorti(event_hist,event_hist_s);
	for(i=1 ; i<=n ; i++ ) {
		ev=substr(event_hist_s[i],1,index(event_hist_s[i],"<")-2);
		ev1=substr(event_hist_s[i+1],1,index(event_hist_s[i+1],"<")-2);
		if(length(ev1)<2)
		{
			ev1=substr(event_hist_s[i+1],1,index(event_hist_s[i+1],">")-2);
		}
		if(event_cnt[ev]<1)
		{
			ev=substr(event_hist_s[i],1,index(event_hist_s[i],">")-2);
			ev1=substr(event_hist_s[i+1],1,index(event_hist_s[i+1],">")-2);
		}
		if(ev!=ev1)
		{
			br=1;
		}
		if(event_hist[event_hist_s[i]]/event_cnt[ev]<1)
		{
			print event_hist_s[i] "(ms)\t\t" event_hist[event_hist_s[i]] "\t" event_hist[event_hist_s[i]]/event_cnt[ev];	
		}
		else
		{
			br=0;
		}
		if(br==1)
		{
			print " ";
			br=0;
		}
		
	}	
	PROCINFO["sorted_in"] = "@val_num_desc";
	for(event in events) {
	  if(events[event]>0) {
		print event " elapsed time (us): " events[event];
	  }
	}
	print " ";
	for(obj in object_wait)
	{
		print obj " elapsed time (us): " object_wait[obj];
	}
	cursor_no_close="END of this parsing";
	cursor_no="END of this parsing";	
	delete event_hist;
	delete event_hist_s;
	delete event_cnt;
	delete events;
	delete object_wait;
	rewinded=0;
	print "---------------------------------------------\n";
  }
}
