#!/bin/bash
export i=0
for n in 3 4 5 6 7 8 9 11 12 13 14 15 16 17 18 40 41; 
do
       export i=$(($i + 1))
       /sbin/ifconfig eth0:$i 10.99.99.$n $1
done;
