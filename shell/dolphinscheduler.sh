#!/bin/bash
if [ $# -lt 1 ]
then
    echo "No Args Input.start、stop"
    exit ;
fi
case $1 in
"start")
/opt/dolphinscheduler/bin/start-all.sh
#/opt/dolphinscheduler/bin/dolphinscheduler-daemon.sh start master-server
#/opt/dolphinscheduler/bin/dolphinscheduler-daemon.sh start worker-server
#/opt/dolphinscheduler/bin/dolphinscheduler-daemon.sh start api-server
#/opt/dolphinscheduler/bin/dolphinscheduler-daemon.sh start logger-server
;;
"stop")
/opt/dolphinscheduler/bin/stop-all.sh
;;
"status")
/opt/dolphinscheduler/bin/status-all.sh
;;
*)
      echo "Input Args Error..."
;;
esac


