#!/bin/sh
# launcher.sh
# navigate to home directory, then to this directory, then execute python script, then back home


LOGFILE=/home/AttAdmin/KardCardRules/logs/restart.log

writelog() {
  now=`date`
  echo "$now $*" >> $LOGFILE
}


while true ; do
  #check for network connectivity
  wget -q --tries=10 --timeout=99 --spider http://google.com
  sleep 3                   # give some time for the timer app to start
  if [ $? -eq 0 ]; then
        cd /home/AttAdmin/KardCardRules
        writelog "Starting"
        sudo python main.py
        writelog "Exited with status $?"
		break
  else
        writelog "No network connection, retrying..."
  fi
done
cd /



