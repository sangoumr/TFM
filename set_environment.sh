# Instal.lar Ubuntu en Windows amb WSL obrir PowerShell y ejecutar:
wsl --install
# Tancar powerShell i obrir Ubuntu des del menú Inici de Windows

# Desactivar ipv6 per conflicte en Ubuntu
sudo sysctl -w net.ipv6.conf.all.disable_ipv6=1
sudo sysctl -w net.ipv6.conf.default.disable_ipv6=1

# Instalar Java versió 11
wget -O - https://apt.corretto.aws/corretto.key | sudo apt-key add - 
sudo add-apt-repository 'deb https://apt.corretto.aws stable main'
sudo apt-get update; sudo apt-get install -y java-11-amazon-corretto-jdk

# Si dona problemes de DNS editar fitxer resolv.conf, i tornar a intentar instal.lar Java, modificant les linies seguents per a que resolgi i per a que no reescrigui el fitxer, i descomentar-les:
sudo nano /etc/resolv.conf
[network]
generateResolvConf = false
nameserver 8.8.8.8

# Comprobar version de Java
java --version

# Instal.lar Kafka (descargar tgz)
wget https://dlcdn.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz

# Extreure contingut i mourel al directori principal
tar -xvzf kafka_2.13-3.9.0.tgz
mv kafka_2.13-3.9.0 ~

# Editar fixer de configuració .bashrc afegint PATH per no posar la ruta sencera de les instruccions de kafka o executar-les desde la carpeta bin
nano .bashrc
PATH="$PATH:~/kafka_2.13-3.9.0/bin"

# Aplicar els canvis
source ~/.bashrc

# Validar modificació path
cat .bashrc

############## Iniciar cluester de Kafka ##############
# Crear nou Cluster Kafka amb ID aleatori
kafka-storage.sh random-uuid

# Configurar directori Logs (amb el ID aleatori retornat al pas anterior)
kafka-storage.sh format -t ID -c ~/kafka_2.13-3.9.0/config/kraft/server.properties
#kafka-storage.sh format -t 55FXLmEVTuqMRdsmyiZ8VA -c ~/kafka_2.13-3.9.0/config/kraft/server.properties

# Inicialitzar Kafka en deamon mode
kafka-server-start.sh ~/kafka_2.13-3.9.0/config/kraft/server.properties

############## Fini inici Kafka ##############

# En cas de voler executar  jupyther notebook desde visual code en entorn (ubuntu)
sudo apt update
sudo apt install python3-ipykernel

# configuraicó python + spark
sudo apt install python3-pip
pip install findspark --break-system-packages

# istallar spark
wget https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
tar -xvzf spark-3.5.5-bin-hadoop3.tgz
mv spark-3.5.5-bin-hadoop3 spark

# afeguir variables entorn de Spark i  Python al fitxer de configuració .bashrc
echo 'export SPARK_HOME=$HOME/spark' >> ~/.bashrc
echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.bashrc
echo 'export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH' >> ~/.bashrc
echo 'export PYSPARK_PYTHON=python3' >> ~/.bashrc
source ~/.bashrc


sudo apt install python3-pandas
sudo apt install python3-kafka

# Crear entorn virtual, activar-lo i afegir llibreries
sudo apt install python3-venv
python3 -m venv myenv
source myenv/bin/activate
pip3 install kafka
pip install --upgrade --force-reinstall kafka-python six
pip install requests



# https://sparkbyexamples.com/pyspark/pyspark-importerror-no-module-named-py4j-java-gateway-error/
# https://support.datastax.com/s/article/Spark-hostname-resolving-to-loopback-address-warning-in-spark-worker-logs


############ Instal.lar hive #############

# afegir a .bashrc variables d'entorn de Java (ja instal.lat previament)
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc

# Aplicar els canvis
source ~/.bashrc

# Installar primer Hadoop (HDFS)
wget https://downloads.apache.org/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz
tar -xzvf hadoop-3.4.1.tar.gz
sudo mv hadoop-3.4.1 /usr/local/hadoop

# Afegir a .bashrc variables entorn Hadoop
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

# Aplicar els canvis
source ~/.bashrc

# Confirmem la instlació de Hadoop mostrant la versió
hadoop version

# Validem HDFS llistant el directori
hdfs dfs -ls /

# Descargem Hive i instal.lem
wget https://downloads.apache.org/hive/hive-4.0.1/apache-hive-4.0.1-bin.tar.gz
tar -xzvf apache-hive-4.0.1-bin.tar.gz
sudo mv apache-hive-4.0.1-bin /usr/local/hive

# afegir a .bashrc variables entorn HIVE
echo 'export HIVE_HOME=/usr/local/hive' >> ~/.bashrc
echo 'export PATH=$HIVE_HOME/bin:$PATH' >> ~/.bashrc
# Aplicar els canvis
source ~/.bashrc

# Confirmem la instlació de Hive mostrant la versió
hive --version

# descomentar i modificar de 1024 a 2048
cp /usr/local/hive/conf/hive-env.sh.template /usr/local/hive/conf/hive-env.sh
nano /usr/local/hive/conf/hive-env.sh
export HADOOP_HEAPSIZE=2048
source /usr/local/hive/conf/hive-env.sh

# Instal.lar servei SSH i crear Keygen i assignar a usuari
sudo apt install openssh-server -y
# validar servei instal.lat
dpkg -l | grep ssh
# verificar port 22 de SSH
grep Port /etc/ssh/sshd_config
# crear psw ssh i assignar als usuaris
ssh-keygen -t rsa -b 4096

''' 
Generating public/private rsa key pair.
Enter file in which to save the key (/home/roser/.ssh/id_rsa):
Enter passphrase (empty for no passphrase):
Enter same passphrase again:
Your identification has been saved in /home/roser/.ssh/id_rsa
Your public key has been saved in /home/roser/.ssh/id_rsa.pub
The key fingerprint is:
SHA256:cLxwzDGM0pXQuZpav9RzWQDkjB06m+Xd1MrDy5rh/Sc roser@Roser-Dell
The key's randomart image is:
+---[RSA 4096]----+
|     ..*+++      |
|    . o+=O o   . |
|     .o X.= . . .|
|       =.B . * . |
|       oS . . B  |
|      +  .   + o |
|     o .. o + o  |
|    .  ..  + =E .|
|        ..  + .oo|
+----[SHA256]-----+
''''

"""cat key_roser.pub >> ~/.ssh/authorized_keys
ssh-copy-id -i key_roser.pub roser@Roser-Dell
ssh-copy-id -i key_roser roser@localhost
"""
# Copiar key a public place (demana psw ubuntu)
ssh-copy-id -i ~/.ssh/id_rsa.pub roser@Roser-Dell
# Validar que s'ha copiat la key a authorized_keys
cat ~/.ssh/authorized_keys
# Donar permisos
cd ~/.ssh/
chmod 600 id_rsa
chmod 600 authorized_keys
chmod 700 ~/.ssh

# Verificar configuració SSH per usar public key
sudo nano /etc/ssh/sshd_config
# descomentar: 
PubkeyAuthentication yes
PasswordAuthentication yes
AuthorizedKeysFile .ssh/authorized_keys

#Reiniciar servei SSH
sudo systemctl restart sshd

------------------INICI Conf Hadoop----------------------------
# https://medium.com/@madihaiqbal606/apache-hadoop-3-3-6-installation-on-ubuntu-22-04-2-lts-wsl-for-windows-bb57ed599bc6

# Configurar java environment variables
# Editar fitxer hadoop conf
nano $HADOOP_HOME/etc/hadoop/hadoop-env.sh
# Descomentar linea i afegir ruta instalació de JAVA
export JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto

# Configurar Hadoop
# Obrir core-site.xml
sudo nano $HADOOP_HOME/etc/hadoop/core-site.xml

# Afegir les seguents lines entre <Configuration> </Configuration>
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


# Create a directory to store node metadata using the following command:
sudo mkdir -p /home/hadoop/hdfs/{namenode,datanode}

# Canviar el propietari del directori (roser) user:
sudo chown -R roser:roser /home/hadoop/hdfs

# Editar el fitxer  hdfs-site.xml:
sudo nano $HADOOP_HOME/etc/hadoop/hdfs-site.xml

# Afeguir les següents propietats entre  <Configuration> </Configuration> i guardar
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
</property>


# Editar el fitxer mapred-site.xml:
sudo nano $HADOOP_HOME/etc/hadoop/mapred-site.xml
# Afeguir les següents propietats entre <Configuration> </Configuration> i guardar
<property>
      <name>mapreduce.framework.name</name>
      <value>yarn</value>
</property>

# Editar el fitxer yarn-site.xml:
sudo nano $HADOOP_HOME/etc/hadoop/yarn-site.xml
# Afeguir les següents propietats entre  <Configuration> </Configuration> i guardar
<property>
      <name>yarn.nodemanager.aux-services</name>
      <value>mapreduce_shuffle</value>
</property>

# Finalment validar la configuració de Hadoop el format the HDFS NameNode:
hdfs namenode -format

# Iniciar el Cluster de Hadoop
start-dfs.sh

# Iniciar el node magager i els recursos manager:
start-yarn.sh

#Verificar que els serveis estan iniciats:
jps
# S'ha de veure així:
"""
2496 ResourceManager
2625 NodeManager
2083 DataNode
2279 SecondaryNameNode
2987 Jps
1309 Kafka
"""


Step 7: Access the Namenode:
http://localhost:9870


Step 8: Access the Hadoop Resource Manager:
http://localhost:8088

---------Fi Conf Hadoop--------------------

# descarregar manual i moure a spark/jars pel consumidor kafka desde pyspark
"""descarregar de:
      https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10_2.12/3.5.0
      https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10_2.12/3.5.5
      https://mvnrepository.com/artifact/org.apache.spark/spark-streaming_2.12/3.5.0
      https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients/3.9.0
"""
mv spark-sql-kafka-0-10_2.12-3.5.0.jar spark/jars/
mv spark-streaming_2.12-3.5.0.jar spark/jars/
mv kafka-clients-3.9.0.jar spark/jars/
mv spark-sql-kafka-0-10_2.12-3.5.5.jar spark/jars/


# Resum modificacions a .bashrc
""" 
PATH="$PATH:~/kafka_2.13-3.9.0/bin"

export HIVE_HOME=/usr/local/hive
export PATH=$HIVE_HOME/bin:$PATH

export HADOOP_HOME=/usr/local/hadoop
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

export JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto
export PATH=$JAVA_HOME/bin:$PATH

export SPARK_HOME=$HOME/spark
export PATH=$SPARK_HOME/bin:$PATH
#export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
export PYSPARK_PYTHON=python3
export SPARK_LOCAL_IP=127.0.0.1
export HADOOP_OPTIONAL_TOOLS=""
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH
export PATH=$PATH:/usr/local/hadoop/sbin
"""


--------NO funciona HIVE-------

#iniciar servei Hive consola per crear nova BBDD i conectar
hive --service hiveserver2 &
hive

#conectar consola al servei i donar d'alta usuari de BBDD
beeline>!connect jdbc:hive2://localhost:10000 username password


"""
roser@Roser-Dell:~$ hive
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/usr/local/hive/lib/log4j-slf4j-impl-2.18.0.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/local/hadoop/share/hadoop/common/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/usr/local/hive/lib/log4j-slf4j-impl-2.18.0.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/local/hadoop/share/hadoop/common/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
Beeline version 4.0.1 by Apache Hive

beeline> !connect jdbc:hive2://localhost:10000
Connecting to jdbc:hive2://localhost:10000
Enter username for jdbc:hive2://localhost:10000: roser
Enter password for jdbc:hive2://localhost:10000: *****
25/03/22 19:08:09 [main]: WARN jdbc.HiveConnection: Failed to connect to localhost:10000
Could not open connection to the HS2 server. Please check the server URI and if the URI is correct, then ask the administrator to check the server status. Enable verbose error messages (--verbose=true) for more information.
Error: Could not open client transport with JDBC Uri: jdbc:hive2://localhost:10000: java.net.ConnectException: Connection refused (Connection refused) (state=08S01,code=0)

beeline> show databases;
No current connection
"""

----------------
# crear nova BBDD
>CREATE DATABASE IF NOT EXISTS TFM;
# validar BBDD creada
>SHOW DATABASES;
---------------


# Per conectar hive amb POWER BI DESKTOP
# aixecar servei Hive
hive --service hiveserver2 &
# conectar driver desde power bi
-> obterner datos -> Hive LLAP ->
localhost:10000
tfm
directquery