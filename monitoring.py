#from subprocess import call
#print call(["ulimit"]).read()
import subprocess
import os
import threading
import re
import time
import datetime
import csv
import argparse
import sys
parser = argparse.ArgumentParser(description='Display stats for multiple nodes')
parser.add_argument('--hosts', type=str, default="127.0.0.1", help='list of machine you want to monitor, eg: 127.0.0.1,127.0.0.2')
parser.add_argument('--user', type=str, default="root", help='SSH user')
parser.add_argument('--key', type=str, default="", help='SSH key path, eg: ~/.ssh/id_rsa')
parser.add_argument('--dump', dest='dump_result', action='store_true', help='dump the result to a local csv file.')
parser.set_defaults(dump_result=False)
parser.add_argument('--dump_to', type=str, default="./monitoring-"+datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H:%M:%S')+".csv",
                    help='save the results to the given file. Not saved if empty')
parser.add_argument('--big-screen', dest='small_screen', action='store_false')
parser.set_defaults(small_screen=True)
parser.add_argument('--exclude-lo', dest='exclude_lo', action='store_true', help="Exclude l0 while reading rx and tx stats. Don't change the total failed/active connection.")
parser.set_defaults(exclude_lo=False)
parser.add_argument('--gc_log_file',  type=str, default="/var/log/cassandra/gc.log", help='gc log file path, eg: "/opt/cassandra/logs/gc.log.*.current".  Change to None or "" to disable')
parser.add_argument('--log_file',  type=str, default="/var/log/system.log", help='cassandra log file path, eg: "/var/log/system.log". Count errors and warns. Change to None or "" to disable')
parser.add_argument('--log_grep_freq',  type=int, default=10, help='error & warn use grep | wc to count errors on the log file. Change this value if you don\'t want to grep too often.')
parser.add_argument('--measure_frequency',  type=int, default=1, help='all other measure frequency (in sec).')

args = parser.parse_args()

if len(args.hosts) == 0:
    sys.exit('Hosts missing. Add host using --host=127.0.0.1,127.0.0.2')
args.hosts = args.hosts.replace(" ", "").split(",")


class BColors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class Host:
    def __init__(self, name):
        self.mutex = threading.RLock()
        self.acq = 0
        self.name = name
        self.devices = []
        self.previous_devices = []
        self.cpu = None
        self.previous_cpu = None
        self.timestamp = -1
        self.previous_timestamp = -1
        self.interfaces = []
        self.previous_interfaces = []
        self.connection_active = -1
        self.previous_connection_active = -1
        self.connection_failed = -1
        self.previous_connection_failed = -1
        self.jvm_stop = -1
        self.error_count = -1
        self.previous_error_count = 0
        self.warn_count = -1
        self.previous_warn_count = 0

    def reset(self):
        self.previous_cpu = self.cpu
        self.cpu = None
        self.previous_devices = self.devices
        self.devices = []
        self.previous_timestamp = self.timestamp
        self.timestamp = -1
        self.previous_interfaces = self.interfaces
        self.interfaces = []
        self.previous_connection_active = self.connection_active
        self.connection_active = -1
        self.previous_connection_failed = self.connection_failed
        self.connection_failed = -1

    def percent_cpu(self):
        if self.cpu is None or self.previous_cpu is None:
            return -1
        total_delta = self.cpu.all() - self.previous_cpu.all()
        idle_delta = self.cpu.all_idle() - self.previous_cpu.all_idle()
        return (total_delta - idle_delta)/total_delta

    def cpu_stat(self, attr):
        if(self.cpu is None or self.previous_cpu is None):
            return -1
        total_delta = self.cpu.all() - self.previous_cpu.all()
        return 1 - (total_delta - getattr(self.cpu, attr) + getattr(self.previous_cpu, attr)) / total_delta

    def all_stat(self, key, stat):
        if len(getattr(self, key)) == 0 or len(getattr(self, "previous_"+key)) == 0:
            return -1
        value = 0
        for item in getattr(self, key):
            value = value + getattr(item, stat)
        for item in getattr(self, "previous_"+key):
            value = value - getattr(item, stat)
        return value

    def all_devices_stat(self, stat):
        return self.all_stat("devices", stat)

    def all_devices_stat_ms(self, stat):
        return self.all_devices_stat(stat) / (self.timestamp - self.previous_timestamp)

    def all_r_await(self):
        if self.all_devices_stat('read_completed') == 0:
            return 0
        return self.all_devices_stat('time_spent_reading') / self.all_devices_stat('read_completed') / (self.timestamp - self.previous_timestamp) * 1000

    def all_w_await(self):
        if self.all_devices_stat('write_completed') == 0:
            return 0
        return self.all_devices_stat('time_spent_writing') / self.all_devices_stat('write_completed') / (self.timestamp - self.previous_timestamp) * 1000

    def all_interfaces_stat(self, stat):
        return self.all_stat("interfaces", stat)


class Cpu:
    regexp= ""
    for i in range(0, 7):
        regexp = regexp + "\s{1,20}(\d{1,20})"
    regexp_cpu_line = re.compile(r'^cpu\s{1,10}'+regexp+'')

    def __init__(self, r):
        self.user = float(r.group(1))
        self.nice = float(r.group(2))
        self.system = float(r.group(3))
        self.idle = float(r.group(3))
        self.iowait = float(r.group(4))
        self.irq = float(r.group(5))
        self.softirq = float(r.group(6))
        self.steal = float(r.group(7))

    def all(self):
        return self.all_non_idle() + self.all_idle()

    def all_idle(self):
        return self.idle + self.iowait

    def all_non_idle(self):
        return self.user + self.nice + self.system + self.irq + self.softirq + self.steal

class Device:
    regexp= ""
    for i in range(0, 11):
        regexp = regexp + "\s{1,20}(\d{1,20})"
    regexp_device_line = re.compile(r'^\s{1,10}\d{1,10}\s{1,10}\d{1,10}\s([0-9a-zA-Z]{1,20})'+regexp+'.*')

    def __init__(self, r):
        #/proc/diskstats as following:
        # 8      16 sdb 828 20 6784 24 0 0 0 0 0 24 24
        # Field  1 -- # of reads completed
        #   This is the total number of reads completed successfully.
        # Field  2 -- # of reads merged, field 6 -- # of writes merged
        #   Reads and writes which are adjacent to each other may be merged for efficiency.
        #   Thus two 4K reads may become one 8K read before it is ultimately handed to the disk, and so it will be counted (and queued) as only one I/O.
        #   This field lets you know how often this was done.
        # Field  3 -- # of sectors read
        #   This is the total number of sectors read successfully.
        # Field  4 -- # of milliseconds spent reading
        #   This is the total number of milliseconds spent by all reads (as measured from __make_request() to end_that_request_last()).
        # Field  5 -- # of writes completed
        #   This is the total number of writes completed successfully.
        # Field  6 -- # of writes merged
        #   See the description of field 2.
        # Field  7 -- # of sectors written
        #   This is the total number of sectors written successfully.
        # Field  8 -- # of milliseconds spent writing
        #   This is the total number of milliseconds spent by all writes (as measured from __make_request() to end_that_request_last()).
        # Field  9 -- # of I/Os currently in progress
        #   The only field that should go to zero. Incremented as requests are given to appropriate struct request_queue and decremented as they finish.
        # Field 10 -- # of milliseconds spent doing I/Os
        #   This field increases so long as field 9 is nonzero.
        # Field 11 -- weighted # of milliseconds spent doing I/Os
        #   This field is incremented at each I/O start, I/O completion, I/O merge, or read of these stats by the number of I/Os in progress (field 9)
        #   times the number of milliseconds spent doing I/O since the last update of this field.  This can provide an easy measure of both
        #   I/O completion time and the backlog that may be accumulating.
        self.name = r.group(1) # 3
        self.read_completed = float(r.group(2)) # 4
        self.read_merged = float(r.group(3)) # 5
        self.sectors_read = float(r.group(4)) # 6
        self.time_spent_reading = float(r.group(5)) # 7
        self.write_completed = float(r.group(6)) # 8
        self.write_merged = float(r.group(7)) # 9
        self.sectors_written = float(r.group(8)) # 10
        self.time_spent_writing = float(r.group(9)) # 11
        self.io_count = float(r.group(10)) # 12
        self.io_time = float(r.group(11)) # 13
        self.weighted_io_time = float(r.group(12)) # 14

class Interface:
    regexp= ""
    for i in range(0, 8):
        regexp = regexp + "\s{1,20}(\d{1,20})"
    regexp_interface_line = re.compile(r'^(\w{1,20})\s{1,20}(\d{1,20})(?:\s{1,20}\d{1,20})?'+regexp+'\s{1,20}(\w{1,20})$')
    regexp_connection_active = re.compile(r'(\d{1,20}) active connections opening')
    regexp_connection_failed= re.compile(r'(\d{1,20}) failed connection attempts')

    def __init__(self, r):
        self.name = r.group(1)
        self.mtu = float(r.group(2))
        self.rx_ok = float(r.group(3))
        self.rx_error = float(r.group(4))
        self.rx_dropped = float(r.group(5))
        self.rx_overrun = float(r.group(6))
        self.tx_ok = float(r.group(7))
        self.tx_error = float(r.group(8))
        self.tx_dropped = float(r.group(9))
        self.tx_overrun = float(r.group(10))
        self.flag = r.group(11)

class HostObserver:
    def __init__(self):
        self.hosts = {}
        for host in args.hosts:
            self.hosts[host] = Host(host)

    regexp_stop_line = re.compile(r'threads were stopped: (\d{1,10},\d{1,10}) seconds')

    def updateHost(self, host_name, lines):
        host = self.hosts[host_name]
        host.mutex.acquire()
        try:
            host.reset()
            in_netstat = False
            #Get the date first. Can't do anything if we don't get the date (might happen if we get errors in the commands).
            for line in lines:
                if line.startswith("__DATE__"):
                    host.timestamp = int(line[len("__DATE__"):-1])
                    date_found = True
            if not date_found:
                host.timestamp = -2
            else:
                for line in lines:
                    if line.startswith("__ERROR__"):
                        host.previous_error_count = host.error_count
                        host.error_count = int(line[len("__ERROR__"):-1])
                    elif line.startswith("__WARN__"):
                        host.previous_warn_count = host.warn_count
                        host.warn_count = int(line[len("__WARN__"):-1])
                    #NETSTAT -i
                    elif(line.startswith("__NETSTAT_START__")):
                        in_netstat = True
                    elif(line.startswith("__NETSTAT_END__")):
                        in_netstat = False
                    elif in_netstat:
                        r = Interface.regexp_connection_active.search(line)
                        if r is not None:
                            host.connection_active = int(r.group(1))
                        r = Interface.regexp_connection_failed.search(line)
                        if r is not None:
                            host.connection_failed = int(r.group(1))

                        r = Interface.regexp_interface_line.search(line)
                        if r is not None:
                            interface = Interface(r)
                            if not args.exclude_lo or interface.name != "lo":
                                host.interfaces.append(interface)
                    else:
                        #jvm stat
                        r = HostObserver.regexp_stop_line.search(line)
                        if r is not None:
                            host.jvm_stop = float(r.group(1).replace(",", "."))*1000
                        else:
                            #diskstat
                            r = Device.regexp_device_line.search(line)
                            if r is not None:
                                host.devices.append(Device(r))
                            else:
                                #cpu stats
                                r = Cpu.regexp_cpu_line.search(line)
                                if r is not None:
                                    host.cpu = Cpu(r)
            if not date_found:
                print "ERROR DATE NOT FOUND"
                for line in lines:
                    print line
        finally:
            host.mutex.release()


def format_int(val, warn, error, align):
    if val>100000:
        txt = ("%.0f" %(val/1000))+"k"
    elif val>1000:
        txt = ("%.2f" %(val/1000))+"k"
    else :
        txt = str(int(val))
    return colorize(val, txt, warn, error, align)

def format_float(val, warn, error, align):
    if val >-0.01 and val < 0:
        val = 0
    if val>100000:
        txt = ("%.0f" %(val/1000))+"k"
    elif val>1000:
        txt = ("%.2f" %(val/1000))+"k"
    elif val>100:
        txt = "%.0f" %val
    else :
        txt = "%.2f" %val
    return colorize(val, txt, warn, error, align)

def colorize(val, txt, warn, error, align):
    txt = txt.ljust(align, " ")
    if(val > error):
        return BColors.FAIL+txt+BColors.ENDC
    if(val > warn):
        return BColors.WARNING+txt+BColors.ENDC
    return txt

observer = HostObserver()

def clean(line):
    if line.endswith('\n'):
        return line[:-1]
    return line

results = {}

def execute_remote_command(host):
    try:
        print 'Initializing connection with host '+args.user+'@'+host+'...'
        gc_command = ""
        error_command = ""
        if args.gc_log_file is not None and args.gc_log_file != "":
            gc_command = '&& tail -n 200 '+args.gc_log_file+' | tac | grep -m 1 "threads were stopped"'
        #TODO: find something more performant
        if args.log_file is not None and args.log_file != "":
            error_command = 'idx=$((idx+1)); if [ $((idx%'+str(args.log_grep_freq)+')) = 0 ] ; then echo "__ERROR__$(grep ERROR '+args.log_file+' | wc -l)" && echo "__WARN__$(grep WARN '+args.log_file+' | wc -l)" ; fi; '
        command = 'unset idx;idx=-1;echo $idx; while true; do echo "IDX=$idx"; ' + error_command + \
                  ' cat /proc/diskstats && cat /proc/stat ' \
                  ' ; echo "__NETSTAT_START__"' \
                  ' && netstat -i' \
                  ' && netstat -s | egrep "(active connections opening|failed connection attempts)"' \
                  ' ; echo "__NETSTAT_END__" ' \
                  ' ; echo "__DATE__$(($(date +%s%N)/1000000))"' + \
                    gc_command + \
                  ' ; echo "__END__" ; sleep '+str(args.measure_frequency)+'; done'
        p = subprocess.Popen("ssh -o StrictHostKeychecking=no "+("" if args.key == "" else "-i "+args.key+" ") +args.user+"@"+host+" '"+ command+"'", shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        #print "ssh -o StrictHostKeychecking=no "+args.user+"@"+host+" '"+ command+"'"
        #print 'Connected to '+args.user+'@'+host+'. Listening updates'
        buffer = []
        while True:
            line = p.stdout.readline()
            if "__END__" in line:
                observer.updateHost(host, buffer)
                buffer = []
            else:
                buffer.append(line)
    except Exception as ex:
        print "connection error with host "+host+"... "+sys.exc_info()[0]+" retry connection in 1 sec..."
    finally:
        print "streaming has stopped for unknwon reason. Retrying in 1 sec..."
        time.sleep(1)
        execute_remote_command(host)


for host in args.hosts:
    t = threading.Thread(target=execute_remote_command, args=(host,))
    t.setDaemon(True)
    t.start()
    time.sleep(0.2)

time.sleep(2)

if args.dump_result and args.dump_to and args.dump_to != "":
    with open(args.dump_to, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(["host","cpu-avg","cpu-user","cpu-nice","avgqu-sz","r/s","sector_r","r_await","w/s","sector_w","w_await","rx_ok","rx_ko","tx_ok","tx_ko","connection_active","connection_fail","last_gc_duration_ms","warning","error"])

while threading.active_count() > 0:
    do_not_print_result = os.system("clear")
    lines_to_print = []
    meta_header_1 = " ".ljust(23, " ")+"CPU (0 -> 1)".ljust(18, " ")+"|"+" I/O (ALL) |".ljust(20, " ")+"ALL DISKS READ".ljust(23, " ")+"|".ljust(10, " ")+"ALL DISKS WRITE".ljust(22, " ")
    short_display_len=len(meta_header_1)
    meta_header_1 += "|".ljust(5, " ")+("NETWORK (ALL INTERFACES, t/rx "+("WITHOUT" if args.exclude_lo else "WITH") +" LO)").ljust(45, " ")+"|    JVM    |  LOGS (last "+str(args.log_grep_freq)+"sec)"
    lines_to_print.append(meta_header_1)
    header = "host".ljust(20, " ")
    header += "avg".ljust(7, " ")
    header += "user".ljust(7, " ")
    header += "nice".ljust(7, " ")+"| "
    header += "avgqu-sz".ljust(10, " ")+"| "
    header += "r/s".ljust(10, " ")
    header += "sector_r".ljust(10, " ")
    header += "r_await".ljust(10, " ")+"| "
    header += "w/s".ljust(10, " ")
    header += "sector_w".ljust(10, " ")
    header += "w_await".ljust(10, " ")+"| "
    header += "rx_ok".ljust(7, " ")
    header += "rx_ko".ljust(7, " ")
    header += "tx_ok".ljust(7, " ")
    header += "tx_ko".ljust(7, " ")
    header += "conn_act".ljust(10, " ")
    header += "conn_fail".ljust(10, " ")+"|"
    header += "last_gc_ms".ljust(11, " ")+"|"
    header += "warn".ljust(10, " ")
    header += "error".ljust(10, " ")
    lines_to_print.append(header)
    lines_to_print.append("".ljust(len(header), "-"))

    for host_name in args.hosts:
        host = observer.hosts[host_name]
        host.mutex.acquire()
        try:
            report = ((host.name+":").ljust(20, " "))
            if host.timestamp == -2:
                report += "ERROR reading timestamp. Check connection/errors. Make sure logs path are correct."
            if host.cpu is None:
                report += "CPU reading error / check connection (will try to reconnect every sec)"
            else:
                report += format_float(host.percent_cpu(), 0.7, 0.9, 7)
                report += format_float(host.cpu_stat('user'), 0.7, 0.9, 7)
                report += format_float(host.cpu_stat('nice'), 0.7, 0.9, 7)
                report += "| "+format_float(host.all_devices_stat_ms('weighted_io_time'), 10, 100, 10)
                report += "| "+format_float((host.all_devices_stat_ms('read_completed')*1000), 500, 5000, 10)
                report += format_int((host.all_devices_stat_ms('sectors_read')*1000), 1000, 10000, 10)
                report += format_float(host.all_r_await(), 30, 100, 10)
                report += "| "+format_float((host.all_devices_stat_ms('write_completed')*1000), 500, 5000, 10)
                report += format_int((host.all_devices_stat_ms('sectors_written')*1000), 1000, 10000, 10)
                report += format_float(host.all_w_await(), 30, 100, 10)
                report += "| "+format_int(host.all_interfaces_stat("tx_ok"), 10000, 100000, 7)
                report += format_int(host.all_interfaces_stat("tx_error") + host.all_interfaces_stat("tx_dropped") + host.all_interfaces_stat("tx_overrun"), 0, 10, 7)
                report += format_int(host.all_interfaces_stat("rx_ok"), 10000, 100000, 7)
                report += format_int(host.all_interfaces_stat("rx_error") + host.all_interfaces_stat("rx_dropped") + host.all_interfaces_stat("rx_overrun"), 0, 10, 7)
                report += format_int(host.connection_active - host.previous_connection_active, 50, 100, 10)
                report += format_int(host.connection_failed - host.previous_connection_failed, 0, 10, 10)
                report += "| "+format_float(host.jvm_stop, 100, 500, 10)
                report += "| "+format_int(host.warn_count - host.previous_warn_count, 0, 1, 10)
                report += format_int(host.error_count - host.previous_error_count, 0, 0, 10)
            lines_to_print.append(report)

            if args.dump_result and args.dump_to and args.dump_to != "":
                for host_name in args.hosts:
                    host = observer.hosts[host_name]
                    with open(args.dump_to, 'a') as csvfile:
                        csv_writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                        csv_writer.writerow([host_name, "%.3f" % host.percent_cpu(), "%.3f" % host.cpu_stat('user'), "%.3f" % host.cpu_stat('nice'), "%.3f" % host.all_devices_stat_ms('weighted_io_time'),
                                             "%.3f" % (host.all_devices_stat_ms('read_completed')*1000), int(host.all_devices_stat_ms('sectors_read')*1000), "%.3f" %host.all_r_await(),
                                             int(host.all_devices_stat_ms('write_completed')*1000), int(host.all_devices_stat_ms('sectors_written')*1000), "%.3f" %host.all_w_await(),
                                             host.all_interfaces_stat("tx_ok"), host.all_interfaces_stat("tx_error") + host.all_interfaces_stat("tx_dropped") + host.all_interfaces_stat("tx_overrun"),
                                             host.all_interfaces_stat("rx_ok"), host.all_interfaces_stat("rx_error") + host.all_interfaces_stat("rx_dropped") + host.all_interfaces_stat("rx_overrun"),
                                             host.connection_active - host.previous_connection_active, host.connection_failed - host.previous_connection_failed, host.jvm_stop,
                                             host.warn_count - host.previous_warn_count, host.error_count - host.previous_error_count])
        finally:
            host.mutex.release()

    if args.small_screen:
        for l in lines_to_print:
            print l[:short_display_len]
        print ""
        for l in lines_to_print:
            print l[len(lines_to_print[0])-short_display_len:]
    else:
        for l in lines_to_print:
            print l


    time.sleep(1)
