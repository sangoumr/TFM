##
# @package set_environment.sh
# Instructions to set up the environment on a single PC.

## Step 1 - Install Ubuntu on Windows

# Instal·lar Ubuntu en Windows amb WSL obrir PowerShell y executar:
wsl --install
# Tancar PowerShell i obrir Ubuntu des del menú Inici de Windows.

# Desactivar ipv6 per conflicte en Ubuntu.
sudo sysctl -w net.ipv6.conf.all.disable_ipv6=1
sudo sysctl -w net.ipv6.conf.default.disable_ipv6=1

## Step 2 - Install Java version 11

# Instal·lar Java versió 11.
wget -O - https://apt.corretto.aws/corretto.key | sudo apt-key add - 
sudo add-apt-repository 'deb https://apt.corretto.aws stable main'
sudo apt-get update; sudo apt-get install -y java-11-amazon-corretto-jdk

# Si dona problemes de DNS editar fitxer resolv.conf, i tornar a intentar instal·lar Java, modificant les línies següents per a que resolgui i per a que no reescrigui el fitxer, i descomentar-les:
sudo nano /etc/resolv.conf
[network]
generateResolvConf = false
nameserver 8.8.8.8

# Comprovar versió de Java.
java --version

# Afegir a .bashrc variables d'entorn de Java (ja instal·lat prèviament).
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc

# Aplicar els canvis.
source ~/.bashrc


## Step 3 - Install Kafka

# Instal·lar Kafka (descarregar tgz).
wget https://dlcdn.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz

# Extreure contingut i moure’l al directori principal.
tar -xvzf kafka_2.13-3.9.0.tgz
mv kafka_2.13-3.9.0 ~

# Editar fitxer de configuració .bashrc afegint PATH per no posar la ruta sencera de les instruccions de Kafka o executar-les des de la carpeta bin.
nano .bashrc
PATH="$PATH:~/kafka_2.13-3.9.0/bin"

# Aplicar els canvis.
source ~/.bashrc

# Validar modificació path.
cat .bashrc

############## Iniciar clúster de Kafka ##############
# Crear nou Cluster Kafka amb ID aleatori.
kafka-storage.sh random-uuid

# Configurar directori Logs (amb el ID aleatori retornat al pas anterior).
kafka-storage.sh format -t ID -c ~/kafka_2.13-3.9.0/config/kraft/server.properties

# Inicialitzar Kafka en deamon mode
kafka-server-start.sh ~/kafka_2.13-3.9.0/config/kraft/server.properties &

# Per parar el servei de Kafka
kafka-server-stop.sh

############## Fini inici Kafka ##############

## Step 4 – Install Visual Studio Code on Ubuntu

# Per parar el servei de Kafka
code .

## Step 5 – (Optional) Run Jupyter Notebook in VSC

# En cas de voler executar  jupyter notebook des de Visual Code en entorn (Ubuntu).
sudo apt update
sudo apt install python3-ipykernel


## Step 6 – Install Python and Sparkk

# Configuració Python + Spark.
sudo apt install python3-pip
pip install findspark --break-system-packages

# Instal·lar Spark.
wget https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
tar -xvzf spark-3.5.5-bin-hadoop3.tgz
mv spark-3.5.5-bin-hadoop3 spark

# Afegir variables entorn de Spark i  Python al fitxer de configuració .bashrc .
echo 'export SPARK_HOME=$HOME/spark' >> ~/.bashrc
echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.bashrc
echo 'export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH' >> ~/.bashrc
echo 'export PYSPARK_PYTHON=python3' >> ~/.bashrc
source ~/.bashrc

sudo apt install python3-pandas
sudo apt install python3-kafka


## Step 7 – Create a virtual environment and install libraries

# Crear entorn virtual, activar-lo i afegir llibreries.
sudo apt install python3-venv
python3 -m venv myenv
source myenv/bin/activate
pip3 install kafka
pip3 install --upgrade --force-reinstall kafka-python six
pip3 install requests
pip3 install setuptools # error Pandas no module named 'distutils'
pip3 install pandas
pip3 install spark-nlp==5.5.3
pip3 install scikit-learn

# https://sparkbyexamples.com/pyspark/pyspark-importerror-no-module-named-py4j-java-gateway-error/
# https://support.datastax.com/s/article/Spark-hostname-resolving-to-loopback-address-warning-in-spark-worker-logs

## Step 8 – Install HIVE and HADOOP (HDFS)

# Instal·lar primer Hadoop (HDFS).
wget https://downloads.apache.org/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz
tar -xzvf hadoop-3.4.1.tar.gz
sudo mv hadoop-3.4.1 /usr/local/hadoop

# Afegir a .bashrc variables entorn Hadoop.
echo 'export HADOOP_HOME=/usr/local/hadoop' >> ~/.bashrc
echo 'export HADOOP_INSTALL=$HADOOP_HOME' >> ~/.bashrc
echo 'export HADOOP_MAPRED_HOME=$HADOOP_HOME' >> ~/.bashrc
echo 'export HADOOP_COMMON_HOME=$HADOOP_HOME' >> ~/.bashrc
echo 'export HADOOP_HDFS_HOME=$HADOOP_HOME' >> ~/.bashrc
echo 'export HADOOP_YARN_HOME=$HADOOP_HOME' >> ~/.bashrc
echo 'export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native' >> ~/.bashrc
echo 'export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin' >> ~/.bashrc
echo 'export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"' >> ~/.bashrc
echo 'export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop' >> ~/.bashrc

# Aplicar els canvis.
source ~/.bashrc

# Confirmem la instal·lació de Hadoop mostrant la versió
hadoop version

# Validem HDFS llistant el directori
hdfs dfs -ls /

# Descarreguem Hive i instal·lem
wget https://downloads.apache.org/hive/hive-4.0.1/apache-hive-4.0.1-bin.tar.gz
tar -xzvf apache-hive-4.0.1-bin.tar.gz
sudo mv apache-hive-4.0.1-bin /usr/local/hive

# Afegir a .bashrc variables entorn HIVE.
echo 'export HIVE_HOME=/usr/local/hive' >> ~/.bashrc
echo 'export PATH=$HIVE_HOME/bin:$PATH' >> ~/.bashrc
# Aplicar els canvis.
source ~/.bashrc

# Confirmem la instal·lació de Hive mostrant la versió.
hive --version

# Descomentar i modificar ‘HADOOP_HEAPSIZE’ de 1024 a 2048.
cp /usr/local/hive/conf/hive-env.sh.template /usr/local/hive/conf/hive-env.sh
nano /usr/local/hive/conf/hive-env.sh
export HADOOP_HEAPSIZE=2048
source /usr/local/hive/conf/hive-env.sh

# Instal·lar servei SSH i crear Keygen i assignar a usuari.
sudo apt install openssh-server –y

# Validar servei instal·lat.
dpkg -l | grep ssh

# Verificar port 22 de SSH.
grep Port /etc/ssh/sshd_config

# Crear psw ssh i assignar als usuaris.
ssh-keygen -t rsa -b 4096

# Copiar key a public place (demana psw ubuntu).
ssh-copy-id -i ~/.ssh/id_rsa.pub roser@Roser-Dell

# Validar que s'ha copiat la key a authorized_keys.
cat ~/.ssh/authorized_keys

# Donar permisos
cd ~/.ssh/
chmod 600 id_rsa
chmod 600 authorized_keys
chmod 700 ~/.ssh

# Verificar configuració SSH per usar public key.
sudo nano /etc/ssh/sshd_config
# descomentar: 
PubkeyAuthentication yes
PasswordAuthentication yes
AuthorizedKeysFile .ssh/authorized_keys

# Reiniciar servei SSH
sudo systemctl restart sshd

## INICI Configuració de Hadoop, es consulta:
# https://medium.com/@madihaiqbal606/apache-hadoop-3-3-6-installation-on-ubuntu-22-04-2-lts-wsl-for-windows-bb57ed599bc6

# Configurar Java environment variables.
# Editar fitxer Hadoop conf.
nano $HADOOP_HOME/etc/hadoop/hadoop-env.sh
# Descomentar línia i afegir ruta instal·lació de JAVA.
export JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto

# Obrir core-site.xml.
sudo nano $HADOOP_HOME/etc/hadoop/core-site.xml
# Afegir les següents línies entre <Configuration> </Configuration>.
<configuration>
 <propiedad>
      <nombre>fs.defaultFS</nombre>
      <valor>hdfs://0.0.0.0:9000</valor>
      <descripción> El URI del sistema de archivos predeterminado</descripción>
   </propiedad>
   <property>
      <name>fs.default.name</name>
      <value>hdfs://0.0.0.0:9000</value>
    </property>
</configuration>

# Crear directori per emmagatzemar les metadades de node:
sudo mkdir -p /home/hadoop/hdfs/{namenode,datanode}

# Canviar el propietari del directori (roser) user:
sudo chown -R roser:roser /home/hadoop/hdfs

# Editar el fitxer  hdfs-site.xml:
sudo nano $HADOOP_HOME/etc/hadoop/hdfs-site.xml
# Afegir les següents propietats entre  <Configuration> </Configuration> i guardar:
<property>
      <name>dfs.replication</name>
      <value>1</value>
   </property>
   <property>
      <name>dfs.name.dir</name>
      <value>file:///home/hadoop/hdfs/namenode</value>
   </property>
   <property>
      <name>dfs.data.dir</name>
      <value>file:///home/hadoop/hdfs/datanode</value>
   <property>
      <name>dfs.webhdfs.enabled</name>
      <value>true</value>
   </property>
</property>

# Editar el fitxer mapred-site.xml:
sudo nano $HADOOP_HOME/etc/hadoop/mapred-site.xml
# Afegir les següents propietats entre <Configuration> </Configuration> i guardar.
<property>
      <name>mapreduce.framework.name</name>
      <value>yarn</value>
</property>

# Editar el fitxer yarn-site.xml:
sudo nano $HADOOP_HOME/etc/hadoop/yarn-site.xml
# Afegir les següents propietats entre  <Configuration> </Configuration> i guardar.
<property>
      <name>yarn.nodemanager.aux-services</name>
      <value>mapreduce_shuffle</value>
</property>

# Finalment validar la configuració de Hadoop el format de HDFS NameNode:
hdfs namenode -format

# Iniciar el Clúster de Hadoop:
start-dfs.sh

# Iniciar el node mànager i els recursos mànager:
start-yarn.sh

#Verificar que els serveis estan iniciats:
jps
# S'ha de veure així:
"""
2178 NodeManager
1845 SecondaryNameNode
2054 ResourceManager
423519 Jps
811 Kafka
1484 NameNode
1614 DataNode
"""

# Access the Namenode:
http://localhost:9870

# Access the Hadoop Resource Manager:
http://localhost:8088

# Tancar els serveis de Hadoop:
stop-yarn.sh
stop-dfs.sh


## Step 9 – Download pre-trained SparkNLP models

# Descarregar models SparkNLP
sudo apt update && sudo apt install unzip -y
# https://sparknlp.org/2025/01/29/berttest_en.html
#copiar a: 
cd TFM/models
mkdir -p ~/models_npl/berttest
unzip berttest_en_5.5.1_3.0_1738112596101.zip -d ~/models_npl/berttest

# https://sparknlp.org/2021/11/21/distilbert_base_sequence_classifier_ag_news_en.html
#copiar a: 
cd TFM/models
mkdir -p ~/models_npl/distilbert
unzip distilbert_base_sequence_classifier_ag_news_en_3.3.3_3.0_1637503060617.zip -d ~/models_npl/distilbert

