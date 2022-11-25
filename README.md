# mtgWebCrawler
Provides scripts for web crawling and processing of MTG trading cards and their information

# TODO:
* Unendlichkeitssymbol entfernen                        ==> DONE
* Airflow DAG machen:
    * Ordner erstellen mtg                              ==> DONE
    * Daten mit crawler lesen und als csv speichern     ==> DONE
    * Daten in HDFS speichern                           ==> DONE
    * Daten in Hive überbringen                         ==> DONE
    * Daten partitionieren                              ==> DONE
    * Daten mit Python von Hive zu MySQL                ==> in progress
* MySQL-Datenbank aufsetzen                             ==> DONE
* Frontend in Flask schreiben                           ==> PARTIALLY

- maybe change pageNum to len(htmls)