#!/bin/sh
outer=1
pyscript="/home/ubuntu/BA_Project/spark/load_only.py"

while [ $outer -le 16 ]
do
    inner=1
    while [ $inner -le 3 ]
    do
        echo dropping cache
        sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
        echo dropped cache
        /home/ubuntu/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --master spark://172.29.0.5:7077 --executor-cores $outer $pyscript --filecount 1 --cores $outer --schema schema
        echo Done with $outer $inner
        inner=$(( inner+1 ))
    done
    echo Done with $outer
    outer=$(( outer+1 ))
done

echo Done
