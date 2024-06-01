#!/bin/bash
# This should be run as a server process only, using in-shell notation ('. server_proc.sh').
# To start the server, run ". server_proc.sh"
# To end/kill the server, add the -k flag: ". server_proc.sh -k"

kill=0

while getopts "k" OPTION;
do
  case "$OPTION" in
    k)
      kill=1;;
  esac
done

if [ $kill -eq 1 ]; then
  pkill -e -f "python3 ivert_server_job_manager.py"
  return 0
else
  # Start the server
  echo "nohup python3 ivert_server_job_manager.py -v >> /mnt/uvol0/ivert_data/ivert_server.log 2>&1 <&- &"
  nohup python3 ivert_server_job_manager.py -v >> /mnt/uvol0/ivert_data/ivert_server.log 2>&1 <&- &
  return 0
fi
