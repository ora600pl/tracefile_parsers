import plotly.offline as py
import plotly.graph_objs as go
import sys
import os
import re
from datetime import datetime
from datetime import timedelta
from plotly import tools

class ListenerLog(object):
    def __init__(self, dirname, name_pattern, date_from, date_to, interval=3600):
        self.dirname = dirname
        self.name_pattern = name_pattern

        conn_data = {}
        df = datetime.strptime(date_from, "%Y%m%d:%H:%M")
        dt = datetime.strptime(date_to, "%Y%m%d:%H:%M")

        host_list = {}

        data_x = []
        for h in range(int((dt - df).total_seconds() / interval)):
            date_x = (df + timedelta(seconds=h*interval)).strftime("%Y%m%d:%H:%M:%S")
            data_x.append(date_x)
            conn_data[date_x] = {}

        for fname in os.listdir(self.dirname):
            if fname.find(self.name_pattern) >= 0:
                listener_log = open(self.dirname + "/" + fname, "r").readlines()
                for log_line in listener_log:
                    log_fields = log_line.split("*")

                    if len(log_fields) > 3 and log_fields[-3].strip() == "establish":
                        date = datetime.strptime(log_fields[0].strip(), "%d-%b-%Y %H:%M:%S")
                        date_bucket = datetime.fromtimestamp(date.timestamp() - (date.timestamp() % interval))


                        if date <= dt and date >= df:
                            hostname = log_fields[-4].strip()
                            hostname = hostname[hostname.find("HOST=")+5:]
                            hostname = hostname[:hostname.find(")(")]
                            ds = date_bucket.strftime("%Y%m%d:%H:%M:%S")

                            host_list[hostname] = 1

                            if conn_data.get(ds):
                                if conn_data[ds].get(hostname):
                                    conn_data[ds][hostname] += 1
                                else:
                                    conn_data[ds][hostname] = 1
                            else:
                                conn_data[ds] = {}
                                conn_data[ds][hostname] = 1

        self.conn_data = conn_data
        self.dt = dt
        self.df = df
        self.data_x = data_x
        self.host_list = host_list

    def plot(self):
        conn_data = self.conn_data

        data_x = self.data_x
        data_y = {}
        fig = go.Figure()

        for time in self.data_x:
            for host in self.host_list:
                if not conn_data[time].get(host):
                    conn_data[time][host] = 0

        for i in data_x:
            for j in conn_data.get(i):
                data_y.setdefault(j, [])
                data_y[j].append(conn_data[i][j])

        for series in data_y:
            fig.add_trace(go.Scatter(x=data_x,
                                        fill="tozeroy",
                                        y=data_y[series],
                                        name=series,
                                        mode='lines+markers',
                                        line=dict(shape='hv'),
                                        ))

        fig['layout'].update(title='Logons to database / hour based on LISTENER logs')

        py.plot(fig, filename=self.name_pattern + ".html")



if __name__ == '__main__':
    if len(sys.argv) >= 5:
        if len(sys.argv) == 5:
            lsnr = ListenerLog(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
        elif len(sys.argv) == 6:
            lsnr = ListenerLog(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5]))
        #print(lsnr.conn_data)
        lsnr.plot()
    else:
        print("This script by Kamil Stawiarski (@ora600pl) is to help you with visualizing data from multiple listener log files")
        print("Usage:")
        print("python listener_analyzer.py /path/to/logs/ pattern_to_filter_reports_by_name date_from[YYYYMMDD:HH24:MI] date_to[[YYYYMMDD:HH24:MI] {interval default = 3600s}")
        print("You have to install plotly first [pip install plotly]\n")
