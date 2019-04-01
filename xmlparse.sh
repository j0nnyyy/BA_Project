#!/bin/sh
base_in="enwiki-20190220-stub-meta-history"
base_in_ext=".xml.gz"
base_out="wiki_small_"
base_out_ext=".json"
i=6

while [ $i < 27 ]
do
    in=$base_in$i$base_in_ext
    out=$base_out$i$base_out_ext
    python /home/ubunut/BA_Project/xmlparse.py --infile in --outfile out
    echo Done with $i
done

echo Done