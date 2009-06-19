#!/bin/bash 
MOCHIPID=`pgrep -f "sname mochiweb"`; while [ 1 ] ; do NUMCON=`netstat -n | awk '/ESTABLISHED/ ' | wc -l`; MEM=`ps -o rss= $MOCHIPID`; echo -e "`date`\t`date +%s`\t$MEM\t$NUMCON"; sleep 3; done | tee -a /data/tmp/mochimem.log 
