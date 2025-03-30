

# TFM - Tendència Geopolítica en Temps Real 

Assignatura: M2.978 / Semestre: 2024-2 / Data: 01-06-2025

## Autores:

  * Maria Roser Santacreu Gou - [msantacreugo@uoc.edu](msantacreugo@uoc.edu)

## Descripció del repositori

Els fitxers continguts en aquest repositori són els següents:

* `memoria.pdf`: Memòria del TFM.
* `constants.py`: Constants utilitzades en el projecte.
* `get_news.py`: Descarrega diària de les noticies actuals dels països seleccionats, aplica un preprocessament, i les envia al productor de Kafka segons tòpics configurats. 
* `get_old_news_us.py`: Descarrega els últims 50 anys de les noticies dels Estats Units, del diari New York Times.
* `ko_consumer_news.py`: Recull les dades dels tòpics de Kafka i les emmagatzema a HDFS..............DOES NOT WORK ....................... 
* `main.py`: Executable del projecte.
* `preproces_text.py`: Fitxer que recull funcions de neteja del text de les noticies .......... TO DO ..........
* `producer_news.py`: fitxer que conté les funcions essències per iniciar el productor de Kafka i enviar el flux de dades.
* `requirements.txt`: ..............TO DO .......................
* `set_environment.sh`: Instruccions per a la configuració del entorn en un únic PC.
* `translate.py`: Fitxer que conté el codi per a la traducció d'un idioma a un altre.
* `/data/news/news_**.csv`: Fitxers que contenen les noticies descarregades a diari dels diferents països 
* `/data/old_news/****_*_news_us_old.csv`: Fitxers que contenen les noticies antigues dels Estats Units per any i mes des del 1970. 
* `/data/paisos-ue.csv`: Fitxer CSV que conté els codis ISO de cada país de la unió europea (incloent el Regne Unit), i els codis ISO de les llengües que fan servir a cada país.
* `/data/pais-us.csv`: Fitxer CSV que conté els codi ISO dels Estats Units, i els codis ISO de les llengües que fan servir
* `/data/president/presidents-**.csv`: Fitxers amb els presidents des del 1970 aproximadament, de cada país ** (codi ISO país), amb el nom, data de nomenament i posició del partit al que pertanyien o pertanyen.

## Configuració del projecte per a la seva execució

Un cop l'entorn esta configurat i els serveis iniciats seguint les instruccions del fitxer `set_environment.sh`, per executar el projecte simplement executeu el fitxer main.py:

  `python3 main.py`

El projecte està preparat per descarregar les noticies diàries dels països seleccionats i enviar-les als tòpics de Kafka corresponents amb el productor, a mes de guardar-les temporalment en fitxers CSV a mode de prevenció durant del desenvolupament del projecte, i on el consumer les processarà...


El fitxer constants.py recull els valors de; timeouts, noms de fitxers i de directoris, i configuracions entre d'altres, que poden ser modificats per l'usuari per adaptar l'execució del projecte als seus requeriments. Els valors actuals d'aquestes constants s'han configurat a partir dels objectius i necessitats del projecte.

* `NEWS_API_KEY` : Clau per a la descàrrega de les noticies actuals dels Estatus Units. https://newsapi.org.
* `NEWS_DATA_IO_KEY` : Clau per a la descarrega de noticies actuals per als països membres de la Unió Europea. https://newsdata.io .
* `NEWS_NY_TIME_KEY` : Clau per a la descàrrega de notícies antigues dels Estats Units del New York Times (descarregades des dels 1970). https://api.nytimes.com
* `NEWS_WAIT_TO_DOWNLOAD` : Segons que es queda en espera per a iniciar de nou dels descarregues de noticies actuals.
* `PATH_NEWS` : ruta al directori on s'emmagatzemen les noticies actuals per cada país.
* `PATH_OLD_NEWS` : ruta al directori on s'emmagatzemen les noticies antigues dels Estats Units (fitxers mensuals).
* `PATH_US_ISO` : Fitxer CSV que conté els codi ISO dels Estats Units, i els codis ISO de les llengües que fan servir.
* `PATH_UE_ISO` : Fitxer CSV que conté els codis ISO de cada país de la unió europea (incloent el Regne Unit), i els codis ISO de les llengües que fan servir.
* `LANGUAGE_TRANSLATION` : Idioma al que es traduiran les noticies, per tal de homogeneïtzar els processament de llenguatge natural.
* `ISO_CODES_NO_TRANSLATION` : Codis ISO dels països que no requereixen traducció de les noticies.
* `HEADERS` : Capçalera dels fitxers que contindran les noticies traduïdes.
* `HEADERS_OLD_NEWS` : Capçalera del fitxer que emmagatzemarà les noticies antigues.
* `NEWS_US` : nombre de noticies que es descarregaran de la api diàriament (o segons la configuració de `NEWS_WAIT_TO_DOWNLOAD`), dels Estats Units.
* `NEWS_UE_COUNTRIES` : nombre de noticies que es descarregaran de la API diàriament (o segons la configuració de `NEWS_WAIT_TO_DOWNLOAD`), dels països membres dels estats units.
* `BOOTSTRAP_SERVER` :  Connexió als brokers de Kafka.
* `TOPIC_NAME_UE` : Tema de Kafka on enviar  i rebre el flux de dades (noticies) per als països membres de la Unió Europea.
* `TOPIC_NAME_US` : Tema de Kafka on enviar  i rebre el flux de dades (noticies) per als Estats Units.
* `TOPICS_LIST` : Llista dels tòpic actuals.
* `CHAR_LIMIT_TRANSLATE` : limitació de caràcters per traduir (limitació a la baixa per evitar errors amb Google Translate).



## Vídeo de presentació

Enllaç al vídeo de presentació del TFM: []()
