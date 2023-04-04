#!/bin/bash

source "/vagrant/scripts/common.sh"

function installLocalNifi {
	echo "install Nifi from local file"
	FILE=/vagrant/resources/$NIFI_ARCHIVE
	tar -xvzf $FILE -C /usr/local
}

function installRemoteNifi {
	echo "install Nifi from remote file"
	curl ${CURL_OPTS} -k -o /vagrant/resources/$NIFI_ARCHIVE -O -L $NIFI_MIRROR_DOWNLOAD
	tar -xvzf /vagrant/resources/$NIFI_ARCHIVE -C /usr/local
}

function installNifi {
	if resourceExists $NIFI_ARCHIVE; then
		installLocalNifi
	else
		installRemoteNifi
	fi
}

function setupNifi {
  echo "setup Nifi"
  #Set host 0.0.0.0 per accesso dall'esterno
  sudo sed -i 's/nifi.web.http.host=127.0.0.1/nifi.web.http.host=0.0.0.0/g' /usr/local/nifi-1.13.2/conf/nifi.properties
  #Aumento memoria per errore Out of memory
  sudo sed -i 's/java.arg.2=-Xms512m/java.arg.2=-Xms1024m/g' /usr/local/nifi-1.13.2/conf/bootstrap.conf
  sudo sed -i 's/java.arg.3=-Xms512m/java.arg.3=-Xms1024m/g' /usr/local/nifi-1.13.2/conf/bootstrap.conf
}

installNifi
setupNifi

echo "Nifi setup complete"