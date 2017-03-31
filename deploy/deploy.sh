#!/bin/bash

SERVICE_NAME=sftpreader
JAR_FILE=sftpreader-1.0-SNAPSHOT.jar
JENKINS_URL=https://build.endeavourhealth.net/job/sftpreader/lastSuccessfulBuild/artifact/target/
JENKINS_USER=deployuser
JENKINS_PASS=
DOWNLOAD_LOCATION=/home/sysadmin/sftpreader/
DEPLOY_LOCATION=/opt/sftpreader/

echo "> Starting deployment of $SERVICE_NAME"

# check running as root
if [[ $EUID > 0 ]]
  then echo "ERROR: Please re-run as root, halting"
  exit
fi

# remove the old jar from download location
if [ -f $DOWNLOAD_LOCATION/$JAR_FILE ]
then
	sudo rm -f $DOWNLOAD_LOCATION/$JAR_FILE
fi

echo
echo "> Getting the latest build from Jenkins"
echo
wget --directory-prefix=$DOWNLOAD_LOCATION --auth-no-challenge --no-check-certificate --http-user=$JENKINS_USER --http-password=$JENKINS_PASS $JENKINS_URL/$JAR_FILE

if [ ! -f $DOWNLOAD_LOCATION/$JAR_FILE ]
then
	echo "ERROR: Could not find downloaded file, halting"
	exit
fi

echo "> Stopping service $SERVICE_NAME"
echo
sudo service $SERVICE_NAME stop
echo

sudo rm -f $DEPLOY_LOCATION/$JAR_FILE

echo "> Deploying new JAR to $DEPLOY_LOCATION"
echo
sudo mv $DOWNLOAD_LOCATION/$JAR_FILE $DEPLOY_LOCATION/

echo "> Starting service $SERVICE_NAME"
echo
sudo service $SERVICE_NAME start
echo

echo "> Complete deployment of $SERVICE_NAME"
echo
