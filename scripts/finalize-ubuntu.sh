#!/bin/bash

source "/vagrant/scripts/common.sh"

function customInstallation {
	sudo cp -f /vagrant/resources/custom/key.pub /root/.ssh/authorized_keys
    
	#apt-get upgrade -y
	#apt update
	#apt install software-properties-common -y 
	#add-apt-repository ppa:deadsnakes/ppa -y
	#apt-get install python3.7 -y
	sudo apt-get update
	sudo apt-get install software-properties-common -y 
	sudo add-apt-repository ppa:deadsnakes/ppa -y
	sudo apt-get update
	sudo apt-get install python3.7 -y
    sudo apt-get install telnet -y
    sudo apt-get install iputils-ping -y
    sudo apt-get install python3-pip -y
    sudo curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    sudo python3.7 get-pip.py
    pip install --user paramiko
    pip install --user jaydebeapi
    pip install --user memory_profiler
    pip install --user xlrd
    pip install --user python-dateutil
    pip install --user requests
    pip install --user protobuf
    pip install -U setuptools
    pip install --user gtfs-realtime-bindings
    pip install --user zeep
    pip install --user confluent_kafka
    pip install --user hdfs
    #export PYSPARK_PYTHON=/usr/bin/python3.7
	echo "export PYSPARK_PYTHON=/usr/bin/python3.7" >> /etc/profile
	
}

function setupUtilities {
    # update the locate database
    updatedb
}

function setupVIM {
#    Use the full vim version 
    apt-get remove -y vim.tiny
    apt-get install -y vim
}

function setupNetStat {
#   Setup netstat (usful for debug)
    apt-get install -y net-tools
}


echo "finalize ubuntu"
setupUtilities
echo "setup vim"
setupVIM
echo "setup netstat"
setupNetStat
echo "customInstallation"
customInstallation
