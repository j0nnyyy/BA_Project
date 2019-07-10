#!/bin/sh
num=1
authorscript="/home/ubuntu/BA_Project/spark/Python/author_similarity.py"
articlescript="/home/ubuntu/BA_Project/spark/Python/article_similarity.py"
dependencies="/home/ubuntu/BA_Project/spark/Python/jaccard_similarity.py"

/home/ubuntu/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --master spark://172.29.0.5:7077 --py-files $dependencies $authorscript --filenumber 4 --maxval 0.9 --jaccmethod both
/home/ubuntu/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --master spark://172.29.0.5:7077 --py-files $dependencies $authorscript --filenumber 10 --maxval 0.9 --jaccmethod both
/home/ubuntu/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --master spark://172.29.0.5:7077 --py-files $dependencies $authorscript --filenumber 20 --maxval 0.9 --jaccmethod both
