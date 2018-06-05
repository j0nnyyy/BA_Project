# BA_Project

1. Download Wikipedia-Dump file **enwiki-*.xml.bz2** (https://dumps.wikimedia.org/enwiki/)
2. Convert downloaded archived XML to JSON by executing the following command in terminal:

    **bzcat enwiki-*.xml.bz2 | scripts/w2j.py > output.json**
    
    or to convert limited size of the dump file (for example up to 10MB) by the following command: 
    
      **bzcat enwiki-*.xml.bz2 | scripts/w2j.py | scripts/split.py 10M wiki.json**
      
3. **main.py** loads converted JSON file into Apache Spark and contains some manipulations with Spark Dataframe
