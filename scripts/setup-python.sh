#!/bin/bash

source "/vagrant/scripts/common.sh"

function installLocalPython {
	echo "install python from local file"
	FILE=/vagrant/resources/$PYTHON_ARCHIVE
	tar -xzf $FILE -C /usr/local
}

function installRemotePython {
	echo "install Python from remote file"
	curl ${CURL_OPTS} -o /vagrant/resources/$PYTHON_ARCHIVE -O -L $PYTHON_MIRROR_DOWNLOAD
	tar -xzf /vagrant/resources/$PYTHON_ARCHIVE -C /usr/local
}

function installPython {
	if resourceExists $PYTHON_ARCHIVE; then
		installLocalPython
	else
		installRemotePython
	fi


	#apt-get upgrade -y
	#apt update
	#apt install software-properties-common -y 
	#add-apt-repository ppa:deadsnakes/ppa -y
	#apt-get install python3.7 -y
	#sudo apt-get install python3.7 -y
    #sudo apt-get install python3-pip -y
    #sudo curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    #sudo python3.7 get-pip.py

	sudo apt-get update
	sudo apt-get install software-properties-common -y 
	sudo add-apt-repository ppa:deadsnakes/ppa -y
	sudo apt-get update
    sudo apt-get install libgdal-dev -y
    sudo apt-get install wget -y
    sudo apt-get install build-essential checkinstall -y
	sudo apt-get install libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libffi-dev zlib1g-dev -y
	#sudo wget https://www.python.org/ftp/python/3.8.12/Python-3.8.12.tgz
	#sudo tar xzf Python-3.8.12.tgz
	#sudo ./Python-3.8.12/configure --enable-optimizations
	sudo /usr/local/Python-$PYTHON_VERSION/configure --enable-optimizations	
	sudo make altinstall
    sudo apt-get install telnet -y
    sudo apt-get install iputils-ping -y
    #pip install --user paramiko
    #pip install --user jaydebeapi
    #pip install --user memory_profiler
    #pip install --user xlrd
    #pip install --user python-dateutil
    #pip install --user requests
    #pip install --user protobuf
    #pip install -U setuptools
    #pip install --user gtfs-realtime-bindings
    #pip install --user zeep
    #pip install --user confluent_kafka
    #pip install --user hdfs
    #export PYSPARK_PYTHON=/usr/bin/python3.7



}

echo "setup python"

installPython
setupPython

echo "python setup complete"
