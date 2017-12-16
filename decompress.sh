#!/bin/bash
n=19
while [ "$n" -lt 28 ]
do
filename="/root/hw1/input/imp.201310$n.txt.bz2"
bzip2 -dk $filename
let "n=n+1"
done
