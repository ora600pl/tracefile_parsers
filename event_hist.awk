/(db file scattered read)|(db file sequential read)|(direct path read)/ {
	event_part=substr($0,index($0,"'")+1); 
	event_name=substr(event_part,1,index(event_part,"'")-1); 
	event_time1=substr($0,index($0,"ela= ")+5);
	event_time=substr(event_time1,1,index(event_time1,"file")-1);
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
END {
	br=0;
	n=asorti(event_hist,event_hist_s);
	for(i=1 ; i<=n ; i++ ) {
		ev=substr(event_hist_s[i],1,index(event_hist_s[i],"<")-2);
		ev1=substr(event_hist_s[i+1],1,index(event_hist_s[i+1],"<")-2);
		if(event_cnt[ev]<1)
		{
			ev=substr(event_hist_s[i],1,index(event_hist_s[i],">")-2);
			br=1;
		}
		else if(ev!=ev1)
		{
			br=1;
		}
		print event_hist_s[i] "(ms)\t\t" event_hist[event_hist_s[i]] "\t" event_hist[event_hist_s[i]]/event_cnt[ev];	
		if(br==1)
		{
			print " ";
			br=0;
		}
		
	}	

	PROCINFO["sorted_in"] = "@val_num_desc";
		
	for(event in events) {
		print event " elapsed time (us): " events[event];
	}


	print " ";
        for(obj in object_wait)
        {
                print obj " elapsed time (us): " object_wait[obj];
        }

	
}
