# cassandra-troubleshooting

## monitoring
Distributed monitoring accross multiple machine. 

Do not require any dependence (read data from `/proc/`. `netstat` must be present for networking). Don't require to be root or sudoer.

* cpu
* IO (global)
* IO (write)
* IO (read)
* Network (read)
* Cassandra JVM young generation (ms) 
* Cassandra errors and logs 

usage: `python monitoring.py --big-screen --user=root --key="~/.ssh/id_rsa" --hosts="127.0.0.1,127.0.0.2,127.0.0.3"`

extra parameters/configuration: `python monitoring.py --help`

![alt tag](https://raw.githubusercontent.com/QuentinAmbard/cassandra-troubleshooting/master/doc/monitoring.png)


## check-cassandra-config
Check all cassandra configuration files of the cluster and make sure they are all the same. Can be used with any file (change the default files with `--files=....`)

usage : `python check-cassandra-config.py --user=root --key="~/.ssh/id_rsa" --hosts="127.0.0.1,127.0.0.2,127.0.0.3"`

require pyyaml to analyse yaml files. pyyaml is included in the yaml folder, download it or install pyyaml (`pip install pyyaml`) on the machine you launch the script (see http://pyyaml.org/).

If you have/don't want to use pyyaml, add the --no-yaml flag : `python check-cassandra-config.py --no-yaml --user=root --key="~/.ssh/id_rsa" --hosts="127.0.0.1,127.0.0.2,127.0.0.3"`

extra parameters/configuration: `python check-cassandra-config.py --help`


![alt tag](https://raw.githubusercontent.com/QuentinAmbard/cassandra-troubleshooting/master/doc/check-configuration.png)


## check-os-config
Make sure all os have the recommended production settings for cassandra (see https://docs.datastax.com/en/landing_page/doc/landing_page/recommendedSettingsLinux.html)

usage : `python check-os-config.py --user=root --key="~/.ssh/id_rsa" --hosts="127.0.0.1,127.0.0.2,127.0.0.3"`

extra parameters/configuration: `python check-cassandra-config.py --help`


![alt tag](https://raw.githubusercontent.com/QuentinAmbard/cassandra-troubleshooting/master/doc/check-os-settings.png)

