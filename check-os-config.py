import subprocess
import threading
import re
import argparse
import sys

parser = argparse.ArgumentParser(description='Check os configuration on multiple nodes.')
parser.add_argument('--user',  type=str, default="root", help='SSH user')
parser.add_argument('--hosts',  type=str, default="127.0.0.1", help='list of machine you want to monitor, eg: 127.0.0.1,127.0.0.2')
parser.add_argument('--key',  type=str, default="", help='SSH key path, eg: ~/.ssh/id_rsa')
parser.add_argument('--disks',  type=str, default="sda", help='list of disks to check, ex: sda,sdb')
parser.add_argument('--local', dest='local_check', action='store_true', help='Execute check locally. Won\'t open a ssj connection')
parser.set_defaults(local_check=False)

args = parser.parse_args()


if len(args.hosts) == 0:
    sys.exit('Hosts missing. Add host using --host=127.0.0.1')

args.hosts = args.hosts.replace(" ", "").split(",")
disks = []
for disk in args.disks.replace(" ", "").split(","):
    disks.append({"disk": disk})
print(disks)

commands = {
    #Network checks
    "network": {"commands": [
        {"name": "net.core.rmem_max", "command": "/sbin/sysctl net.core.rmem_max","contains": "=\s?16777216$"},
        {"name": "net.core.wmem_max", "command": "/sbin/sysctl net.core.wmem_max","contains": "=\s?16777216$"},
        {"name": "net.core.rmem_default" ,"command": "/sbin/sysctl net.core.rmem_default","contains": "=\s?16777216$"},
        {"name": "net.core.wmem_default", "command": "/sbin/sysctl net.core.wmem_default","contains": "=\s?16777216$"},
        {"name": "net.core.optmem_max", "command": "/sbin/sysctl net.core.optmem_max","contains": "=\s?40960$"},
        {"name": "net.ipv4.tcp_rmem", "command": "/sbin/sysctl net.ipv4.tcp_rmem","contains": "=\s?4096\s87380\s16777216$"},
        {"name": "net.ipv4.tcp_wmem", "command": "/sbin/sysctl net.ipv4.tcp_wmem","contains": "=\s?4096\s87380\s16777216$"},
        {"name": "vm.max_map_count", "command": "/sbin/sysctl vm.max_map_count","contains": "=\s?1048575$"},
        {"name": "net.ipv4.tcp_moderate_rcvbuf", "command": "/sbin/sysctl net.ipv4.tcp_moderate_rcvbuf","contains": "=\s?1$"},
        {"name": "net.ipv4.tcp_no_metrics_save", "command": "/sbin/sysctl net.ipv4.tcp_no_metrics_save","contains": "=\s?1$"},
        {"name": "net.ipv4.tcp_mtu_probing", "command": "/sbin/sysctl net.ipv4.tcp_mtu_probing","contains": "=\s?1$"},
        {"name": "net.core.default_qdisc", "command": "/sbin/sysctl net.core.default_qdisc","contains": "=\s?fq$"}
    ]},
    #Memory checks
    "memory": {"commands": [
        {"name": "vm.min_free_kbytes", "command": "/sbin/sysctl vm.min_free_kbytes","contains": "=\s?1048576$"},
        {"name": "vm.dirty_background_ratio", "command": "/sbin/sysctl vm.dirty_background_ratio","contains": "=\s?5$"},
        {"name": "vm.dirty_ratio", "command": "/sbin/sysctl vm.dirty_ratio","contains": "=\s?10$"},
        {"name": "vm.zone_reclaim_mode", "command": "/sbin/sysctl vm.zone_reclaim_mode","contains": "=\s?0$"},
        {"name": "vm.swappiness", "command": "/sbin/sysctl vm.swappiness","contains": "=\s1$"},
        {"name": "swap off", "command": "free", "contains": "Swap:\s*0\s*0\s*0"},
        {"name": "transparent_hugepage defrag", "command": "cat /sys/kernel/mm/transparent_hugepage/defrag", "contains": "\[never\]"},
        {"name": "scaling_governor should be disabled or set to performance", "command": "cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor", "contains": "(performance|)"}
        
        #{"name": "transparent_hugepage disabled", "command": "cat /sys/kernel/mm/transparent_hugepage/enabled", "contains": "\[never\]"}
    ]},
    #SSD checks
   "ssd": {
       "vars": disks, #"vars": [{"disk": "sda1"}, {"disk": "sda2"}],
       "commands": [
        #{"name": "disks 4k blocks", "command": "fdisk -l /dev/{disk}","contains": "I/O size (minimum/optimal): 4096 bytes / 4096 bytes"},
        {"name": "scheduler deadline", "command": "cat /sys/block/{disk}/queue/scheduler","contains": "\[deadline\]"},
        {"name": "rotational 0", "command": "cat /sys/class/block/{disk}/queue/rotational","equals": "0"},
        {"name": "read ahead 8k", "command": "cat /sys/class/block/{disk}/queue/read_ahead_kb","equals": "8"}
    ]},
    #limits checks
    "ulimits": {"commands": [
        {"name": "Max Locked Memory", "command": 'cat /proc/$DSE_PID/limits',"contains": "Max locked memory\s*unlimited\s*unlimited"},
        {"name": "Max file locks", "command": 'cat /proc/$DSE_PID/limits',"contains": "Max file locks\s*(unlimited|100000)\s*(unlimited|100000)"},
        {"name": "Max processes", "command": 'cat /proc/$DSE_PID/limits',"contains": "Max processes\s*(unlimited|32768)\s*(unlimited|32768)"},
        {"name": "Max resident", "command": 'cat /proc/$DSE_PID/limits',"contains": "Max resident set\s*unlimited\s*unlimited"}
    ]}}

def clean(line):
    if line.endswith('\n'):
        return line[:-1]
    return line

mutex = threading.RLock()

def analyse(host):
    results = {}
    for group_name, config  in commands.items():
        vars = [{}] if "vars" not in config else config["vars"]
        results[group_name] = []
        for var in vars:
            all_command = ""
            for command in config["commands"]:
                if all_command != "":
                    all_command += " ; echo '__SEPARATOR__' ; "
                command_name = "DSE_PID=$(ps -ef | grep DseMod | grep -v grep | awk '{{print $2}}' | head -n 1) ; "+command["command"].format(**var)
                all_command += command_name
            if args.local_check:
                command_full = all_command
            else:
                command_full = 'ssh -qo "StrictHostKeyChecking no" '+("" if args.key == "" else "-i "+args.key+" ") + args.user+'@'+host+' "'+all_command+'"'

            p = subprocess.Popen(command_full, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            lines = p.stdout.readlines()
            command_return = clean("".join(lines))
            command_returns = command_return.split("__SEPARATOR__")
            for idx, command in enumerate(config["commands"]):
                command_name = " "+command["command"].format(**var)
                command_return = command_returns[idx]
                result = {"command": command_name, "value": command_return, "name": command["name"]}
                if "equals" in command:
                    result.update({"type": "equals", "expected": command["equals"]})
                    result["state"] = "error" if command_return.replace("\n", "") != command["equals"].replace("\n", "") else "success"
                elif "contains" in command:
                    result.update({"type": "contains", "expected": command["contains"]})
                    regexp = re.compile(r''+command["contains"])
                    result["state"] = "error" if regexp.search(command_return) is None else "success"
                results[group_name].append(result)
    re.sub('[ES]', 'a', s)
    mutex.acquire()
    try:
        print "--------------------------------------------"
        print "RESULT FOR "+args.user+"@"+host+":"
        print "   "+"configuration".ljust(40, " ")+"\t "+"command".ljust(50, " ")+" \t\t"+"expectation".ljust(50, " ")+"\t\t "+"current value"
        for k, values in results.items():
            print k+" checks"
            error = 0
            for v in values:
                if v["state"] != "success":
                    print "   \033[91m"+(v["state"]+" "+v["name"]+":\033[0m").ljust(40, " ")+"\t "+v["command"].ljust(50, " ")+" \t\t"+v["expected"].ljust(50, " ")+"\t\t "+v["value"].replace("^\s*\n", ' ')
                    error = error +1
            if error == 0:
                print "Ok"
    finally:
        mutex.release()

threads = []
for host in args.hosts:
    print 'Checking host '+args.user+'@'+host+'...'
    t = threading.Thread(target=analyse, args=(host,))
    t.start()
    threads.append(t)
    #time.sleep(0.01)

for t in threads:
    t.join()


