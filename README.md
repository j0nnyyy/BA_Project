# BA_Project
## BA_Project/log
Enthält Logdateien mit Daten zu den Ausführungszeiten der Spark-Applikationen.

## BA_Project/shell-scripts
Kurze Shell-Skripte, um die Ausführung mehrerer Spark-Python-Skripte hintereinander zu ermöglichen

- __for_each_file.sh__: Führt die angegebene Spark-Applikation für jeden Wikipedia-Dump durch
- __start_with_diff_cores.sh__: Führt die angegebene Spark-Applikation mit verschiedener Anzahl von Kernen aus

## BA_Project/spark/Python/mokoiaar:
Enthält die Dateien von Herr Mokoian, dem Ersteller des Original-Repositorys, von welchem dieses Repo geforked wurde. Dies ist erwähnenswert, da die von Herrn Mokoian verfassten Skripte halfen schnell ein Verständnis für Apache Spark zu erlangen. Zudem wurde das __load_to_spark.py__-Skript in etwas veränderter Form zum Laden der Wikipedia-Dumps verwendet.

Inhalt der ursprünglichen readme.md:

1. Download Wikipedia-Dump file **enwiki-*.xml.bz2** (https://dumps.wikimedia.org/enwiki/)
2. Convert downloaded archived XML to JSON by executing **xmlparse.py** script
3. after that it could be used for executing any of the script from spark directory

## BA_Project/spark/Python/seitzjon:
Enthält die für diese Bachelorarbeit implementierten Skripte:

- __active_inactive_count.py__: Ermittelt für jeden Wikipedia-Dump die Anzahl der Benutzer, welche "aktiv" waren, also eine bestimmte Anzahl an Revisionen verfasst haben, bzw. "inaktiv" waren.

- __article_categories_per_similarity.py__: Erstellt Histogramme zur Jaccard-Distanz der Kategorien von Nutzergruppen bestimmter Jaccard-Distanz.

    Parameter:
    - *--filecount*: Anzahl der zu ladenden Dateien
    - *--filenumber*: Dateinummer der zu ladenden Daten
    
- __article_correlation.py__: Erstellt ein Histogramm zur Pearson-Korrelation von Artikelverläufen sowie Histogramme zur Jaccard-Distanz von Artikelgruppen bestimmter Korrelation.

- __article_similarity.py__: Erstellt Histogramme zur Jaccard-Distanz von Artikeln.

    Parameter:
    - *--filecount*: Anzahl der zu ladenden Dateien
    - *--filenumber*: Dateinummer der zu ladenden Datei
    - *--mode*: Steuert Berechnung der Jaccard-Similarität oder Distanz (Werte: *sim*, *dist* (default))
    - *--jaccmethod*: Steuert die benutzte Berechnungsmethode (Werte: *cross*: Crossjoin, *hash*: Min-Hashing (default))
    - *--minval*: Setzt den Minimalen Wert, welchen das Histogramm darstellt
    - *--maxval*: Setzt den Maximalen Wert, welchen das Histogramm darstellt
    
- __articles_per_author.py__: Ermittelt die durchschnittliche Anzahl der Revisionen und Artikel eines Autors, sowie die zugehörige Standardabweichung.

- __articles_per_author_similarity.py__: Berechnet die durchschnittliche Anzahl von Artikeln der Autorengruppen mit bestimmter Jaccard-Distanz.

    Parameter:
    - *--filecount*: Anzahl der zu ladenden Dateien
    - *--filenumber*: Dateinummer der zu ladenden Datei
    
- __author_correlation.py__: Erstellt ein Histogramm zur Pearson-Korrelation von Autorenverläufen sowie Histogramme zur Jaccard-Distanz von Autorengruppen bestimmter Korrelation.

- __author_occurrences_per_hotspot.py__: Erstellt ein Histogramm zur Häufigkeit des Auftretens von Autoren in Artikelhotspots sowie ein Histogramm, welches die Anzahl der Autoren bezüglich der Anzahl verschiedener Artikel, an welchen die Autoren an mindestens einem Hotspot beteiligt waren, darstellt.

- __author_similarity.py__: Erstellt Histogramme zur Jaccard-Distanz von Autoren.

    Parameter:
    - *--filecount*: Anzahl der zu ladenden Dateien
    - *--filenumber*: Dateinummer der zu ladenden Datei
    - *--mode*: Steuert Berechnung der Jaccard-Similarität oder Distanz (Werte: *sim*, *dist* (default))
    - *--jaccmethod*: Steuert die benutzte Berechnungsmethode (Werte: *cross*: Crossjoin, *hash*: Min-Hashing (default))
    - *--minval*: Setzt den Minimalen Wert, welchen das Histogramm darstellt
    - *--maxval*: Setzt den Maximalen Wert, welchen das Histogramm darstellt
    
- __author_title_distribution.py__: Ermittelt das Verhältnis der Autorenanzahl zur Artikelanzahl in den einzelnen Dateien.

- __authors_per_article.py__: Berechnet die durchschnittliche Anzahl der Autoren zu einem Artikel.

- __authors_per_article_similarity.py__: Berechnet die durchschnittliche Anzahl der Autoren zu Artikelgruppen mit bestimmter Jaccard-Distanz.

- __avg_revisions_per_month.py__: Ermittelt die durchschnittliche Anzahl von Revisionen pro Monat, beginnend beim 1.1.1970

- __category_hotspots.py__: Erstellt ein Histogramm zur Jaccard-Distanz zwischen Artikelhotspots und den Hotspots der zugehörigen Kategorien.

- __cross_hash_difference.py__: Ermittelt den Unterschied der Werte der Jaccard-Distanz bei manueller Berechnung und Min-Hashing.

    Parameter:
    - *--filenumber*: Dateinummer der zu ladenden Datei
    - *--filter*: Bestimmt, welche Werte betrachtet werden sollen (Werte: *only_similar*, *only_non_similar*)
    - *--samples*: Anzahl der verwendeten Samples
    
- __cross_hash_time_comparison.py__: Ermittelt die Laufzeiten der manuellen Jaccard-Distanz-Berechnung und des Min-Hashing Ansatzes auf denselben Daten.

    Parameter:
    - *--filenumber*: Dateinummer der zu ladenden Datei
    - *--samples*: Anzahl der verwendeten Samples
    
- __global_hotspots.py__: Ermittelt die Anzahl von Revisionen in bestimmten Intervallen seit dem 01.01.1970.

    - *--filecount*: Anzahl der zu ladenden Dateien
    - *--filenumber*: Dateinummer der zu ladenden Datei
    - *--windowsize*: Größe des betrachteten Zeitintervalls
    - *--multiplier*: Multiplikator für die Identifikation eines Hotspots
    
- __hotspot_correlation.py__: Erstellt Histogramme zur Korrelation von Autoren- bzw. Artikelverläufen.

    Parameter:
    - *--filecount*: Anzahl der zu ladenden Dateien
    - *--filenumber*: Dateinummer der zu ladenden Datei
    - *--compare*: Gibt die zu vergleichenden Werte an (Werte: *author*, *article*)
    
- __hotspot_count.py__: Erstellt ein Histogramm welches die Anzahl der Artikel/Autoren mit bestimmter Hotspotanzahl darstellt.

    Parameter:
    - *--type*: Gibt an ob Autoren- oder Artikelhotspots gezählt werden sollen (Werte: *author*, *title*)
    
- __hotspot_detection.py__: Stellt Funtionen zur Hotspoterkennung bereit.

- __hotspot_weekly_vs_monthly.py__: Erstellt ein Histogramm, welches Hotspots anhand des Zeitintervalls einer Woche und eines Monats vergleicht.

- __hotspots_per_category.py__: Erstellt ein Histogramm, welches angibt, wie viele Kategorien eine bestimmte Hotspotanzahl besitzen.

- __hotspots_per_title_correlation.py__: Erstellt ein Histogramm, welches die Anzahl von Artikel mit Artikelverläufen geringer Jaccard-Distanz darstellt. 

    Parameter:
    - *--filecount*: Anzahl der zu ladenden Dateien
    - *--filenumber*: Dateinummer der zu ladenden Datei
    
- __jaccard_similarity.py__: Stellt Funktionen für die Berechnung der Jaccard-Distanz zur Verfügung.

- __load_only.py__: Ermittelt die Ausführungszeiten der Spark-Ladeoperation auf Basis der gegebenen Parameter.

    Parameter:
    - *--filecount*: Anzahl der zu ladenden Dateien
    - *--schema*: Gibt, sofern gesetzt, an ob ein Dateischema verwendet werden soll (Werte: jegliche Werte führen zur Ausführung mit vorgegebenem Dateischema)
    
- __load_to_spark.py__: Stellt Funktionen zum Laden von Wikipedia-Dumps zur Verfügung.

- __performancetest.py__: Ermittelt die Ausführungszeit verschiedener Spark-Transformationen.

    Parameter:
    - *--filecount*: Anzahl der zu ladenden Dateien
    - *--cores*: Anzahl der zu verwendenden Kerne
    - *--same*: Gibt an, ob Spark-Master und Spark-Slave auf einem Knoten laufen
    
- __similar_author_article_count.py__: Ermittelt die Anzahl der Artikel von Autoren mit geringer Jaccard-Distanz.

    Parameter:
    - *--filenumber*: Dateinummer der zu ladenden Datei
    
    
