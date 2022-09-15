#!/bin/bash
if [ $# -lt 1 ]
then
    echo "No Args Input.start、stop"
    exit ;
fi
case $1 in
"start")
        echo " =================== 启动 azkaban==================="
         ssh root@node1 "xcall /opt/azkaban/azkaban-exec/bin/start-exec.sh"
         curl -G "node1:12321/executor?action=activate" && echo
         curl -G "node2:12321/executor?action=activate" && echo
         curl -G "node3:12321/executor?action=activate" && echo
         ssh root@node1 "/opt/azkaban/azkaban-web/bin/start-web.sh"
;;
"stop")
        echo " =================== 关闭 azkaban ==================="
        ssh root@node1 "/opt/azkaban/azkaban-web/bin/shutdown-web.sh"
        ssh root@node1 "xcall /opt/azkaban/azkaban-exec/bin/shutdown-exec.sh"

;;
*)
    echo "Input Args Error..."
;;
esac

