import sys

import threading
import subprocess
import time
import argparse



parser = argparse.ArgumentParser(description='Check DSE configuration file on multiple nodes.')
parser.add_argument('--no-yaml', dest='enable_yaml', action='store_false', help="disable advanced yaml check, just do checksum like other files.")
parser.set_defaults(no_yaml=True)
parser.add_argument('--user',  type=str, default="root", help='SSH user')
parser.add_argument('--key',  type=str, default="~/.ssh/bootcamp", help='SSH key path, eg: ~/.ssh/bootcamp')
parser.add_argument('--hosts',  type=str, default="127.0.0.1", help='list of machine you want to monitor, eg: 127.0.0.2,127.0.0.1')
parser.add_argument('--files',  type=str, default="/etc/dse/cassandra/cassandra.yaml,/etc/dse/dse.yaml,/var/lib/datastax-agent/conf/address.yaml,/etc/dse/dse-env.sh,/etc/default/dse,/etc/dse/cassandra/cassandra-rackdc.properties,/etc/dse/cassandra/jvm.options,/etc/dse/cassandra/cassandra-env.sh,/etc/dse/cassandra/cqlshrc.default,/etc/dse/cassandra/logback.xml,/etc/dse/cassandra/jmxremote.password,/etc/dse/cassandra/hotspot_compiler,/etc/dse/spark/spark-env.sh,/etc/dse/spark/dse-spark-env.sh,/etc/dse/spark/java-opts,/etc/dse/spark/spark-defaults.conf",
                    help='list of files to compare, eg: /etc/dse/cassandra/cassandra.yaml,/etc/dse/dse.yaml,/var/lib/datastax-agent/conf/address.yaml')
args = parser.parse_args()

if args.enable_yaml:
    import yaml

if len(args.hosts) == 0:
    sys.exit('Hosts missing. Add host using --hosts=127.0.0.1')

args.hosts = args.hosts.replace(" ", "").split(",")
args.files = args.files.replace(" ", "").split(",")

results = {}

class BColors:
    BRed = '\033[41m'
    BGreen = '\033[42m'
    BYellow = '\033[43m'
    BBlue = '\033[44m'
    BMagenta = '\033[45m'
    BCyan = '\033[46m'

    Grey = '\033[90m'
    Red = '\033[91m'
    Green = '\033[92m'
    Yellow = '\033[93m'
    Blue = '\033[94m'
    Magenta = '\033[95m'
    Cyan = '\033[96m'
    White = '\033[97m'
    Default = '\033[99m'
    ENDC = '\033[0m'

#colors = [BColors.BRed,BColors.BGreen,BColors.BBlue,BColors.BMagenta,BColors.BCyan, BColors.Green, BColors.Blue, BColors.Cyan, BColors.Yellow, BColors.Magenta, BColors.Grey]
colors = [BColors.Green, BColors.Blue, BColors.Cyan, BColors.Yellow, BColors.Magenta, BColors.Grey]

#Execute the given command on all the nodes, asynch. Call the given method as soon as une ssh answer.
def executeForAllHostAsynch(command, method, wait = True):
    def collect(host,command):
        command = 'ssh -o "StrictHostKeyChecking no" '+("" if args.key == "" else "-i "+args.key+" ") + args.user+'@'+host+' "'+command+'"'
        p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        lines = p.stdout.readlines()
        method(host, lines)

    threads = []
    for host in args.hosts:
        t = threading.Thread(target=collect, args=(host,command,))
        t.start()
        threads.append(t)
        time.sleep(0.2)
    if wait:
        for t in threads:
            t.join()
    return threads

def compare_checksum(file):
    command = "md5sum "+file+" | cut -d' ' -f 1"
    md5 = {}
    def process(host, lines):
        md5[host] = "".join(lines).replace("\n", "")
    executeForAllHostAsynch(command, process)

    error = False
    for host in args.hosts:
        if md5[host] != md5[args.hosts[0]]:
            error = True
    if error:
        print BColors.Red+file+" ARE DIFFERENTS:"+BColors.ENDC+" (displaying file md5)"
        line = ""
        color_used = {}
        for host in args.hosts:
            if md5[host] in color_used.keys():
                color = color_used[md5[host]]
            else:
                color = colors[min(len(colors)-1, len(color_used))]
                color_used[md5[host]] = color
            line += (host+":"+color+md5[host][:10]+BColors.ENDC).ljust(35, " ")
        print line
    else:
        print file+" ARE IDENTICAL ON ALL MACHINES"

def compare_yaml(file):
    mutex = threading.RLock()
    all_keys = set()
    host_params = {}
    files = {}

    def build_keys(host, yaml_file, suffix):
        for key in yaml_file:
            if type(yaml_file[key]) == type(dict()):
                build_keys(host, yaml_file[key], suffix+key+".")
            else:
                all_keys.add(suffix+key)
                host_params[host][suffix+key] = yaml_file[key]

    def process(host, lines):
        command_return = "\n".join(lines)
        yaml_file = yaml.load(command_return)
        mutex.acquire()
        try:
            files[host] = yaml_file
            host_params[host] = {}
            build_keys(host, yaml_file, "")
        finally:
            mutex.release()

    executeForAllHostAsynch('cat '+file, process)

    key_error = set()
    color_used = {}
    for k in all_keys:
        color_used[k] = {}
        hash_val = 0
        for host in args.hosts:
            if k not in host_params[host]:
                key_error.add(k)
            else:
                if hash_val == 0:
                    hash_val = hash(str(host_params[host][k]))
                if hash_val != hash(str(host_params[host][k])):
                    key_error.add(k)

    if len(key_error) == 0:
        print file+" FILES ARE IDENTICAL ON ALL MACHINES"
    else:
        print BColors.Red+file+" ARE DIFFERENTS:"+BColors.ENDC
        header = "host".ljust(20, " ")
        for k in sorted(key_error):
            header += (k[:19]+"." if len(k) > 20 else k).ljust(20, " ")+"|"
        print header

        for host in args.hosts:
            line = host.ljust(20, " ")
            for k in sorted(key_error):
                if k not in host_params[host]:
                    line += BColors.Red+"MISSING".ljust(20, " ")+BColors.ENDC+"|"
                else:
                    val = host_params[host][k]
                    if val in color_used[k].keys():
                        color = color_used[k][val]
                    else:
                        color = colors[min(len(colors)-1, len(color_used[k]))]
                        color_used[k][val] = color
                    line += color + (str(host_params[host][k])).ljust(20, " ") + BColors.ENDC+"|"
            print line


for file in args.files:
    if args.enable_yaml and (file[-4:] == "yaml" or file[-3:] == "yml"):
        compare_yaml(file)
    else:
        compare_checksum(file)
