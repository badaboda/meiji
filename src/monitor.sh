#!/usr/bin/sh     
MOCHIPID=`pgrep -f 'mochiconntest'`; while [ 1 ] ; do  MEM=`ps -o rss= $MOCHIPID`; echo -e "`date`\t`date +%s`\t$MEM"; sleep 3; done | tee -a mochimem.log 
