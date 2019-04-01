#!/bin/sh
max_count=$1
count=1
pyscript="/home/ubuntu/BA_project/spark/performancetest.py"

while [ count < max_count ]
do
    ./home/ubuntu/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --master spark://172.29.0.5:7077 $pyscript --file_count $count
    ((count++))
    echo Done with $count
done

echo Done