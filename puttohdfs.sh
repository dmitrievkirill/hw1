#!/bin/bash
hadoop fs -mkdir /hw1/
hadoop fs -mkdir /hw1/input/
n=19
while [ "$n" -lt 28 ]
do
filename="/root/hw1/input/imp.201310$n.txt"
hadoop fs -copyFromLocal $filename /hw1/input/
let "n=n+1"
done
hadoop fs -copyFromLocal /root/hw1/city.en.txt /hw1/ 
hadoop fs -copyFromLocal /root/hw1/region.en.txt /hw1/
