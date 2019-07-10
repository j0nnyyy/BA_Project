#!/bin/sh
num=1
authorscript="/home/ubuntu/BA_Project/spark/Python/author_similarity.py"
articlescript="/home/ubuntu/BA_Project/spark/Python/article_similarity.py"
dependencies="/home/ubuntu/BA_Project/spark/Python/jaccard_similarity.py"

while [ $num -le 26 ]
do
    /home/ubuntu/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --master spark://172.29.0.5:7077 --py-files $dependencies $pyscript --filenumber $num
    num=$(( num+1 ))
    echo Done with $num
done
echo Done
