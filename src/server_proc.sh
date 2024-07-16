#!/bin/bash
# To start the server, run "server_proc.sh"
# To end/kill the server, add the -k flag: "server_proc.sh -k"
# To list the process running, add the -l flag: "server_proc.sh -l"

while getopts "kl" OPTION;
do
  case "$OPTION" in
    k)
      echo "pkill -e -f 'python3 maintain_server_manager.py'"
      pkill -e -f "python3 maintain_server_manager.py"
      echo "pkill -e -f 'python3 ivert_server_job_manager.py'"
      pkill -e -f "python3 ivert_server_job_manager.py"
      exit 0
      ;;
    l)
      pgrep -a python3 | grep -e maintain_server_manager\.py -e ivert_server_job_manager\.py
      exit 0
      ;;
    *)
      echo "Unknown option"
      exit 1
      ;;
  esac
done

# Echo the command back to the user.
echo "nohup python3 maintain_server_manager.py -v >> /mnt/uvol0/ivert_data/ivert_server.log 2>&1 <&- &"
# Start the server
nohup python3 maintain_server_manager.py -v >> /mnt/uvol0/ivert_data/ivert_server.log 2>&1 <&- &

