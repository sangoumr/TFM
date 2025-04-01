#............... Iniciar cluester de Kafka ................#
# Crear nou Cluster Kafka amb ID aleatori
kafka-storage.sh random-uuid

# Configurar directori Logs (amb el ID aleatori retornat al pas anterior)
kafka-storage.sh format -t ID -c ~/kafka_2.13-3.9.0/config/kraft/server.properties
#kafka-storage.sh format -t gzpRRxmnSeOq0a734of4cw -c ~/kafka_2.13-3.9.0/config/kraft/server.properties

# Inicialitzar Kafka en deamon mode
kafka-server-start.sh ~/kafka_2.13-3.9.0/config/kraft/server.properties
#................ Fini inici Kafka ...............#

#Reiniciar servei SSH
sudo systemctl restart ssh

#................. Iniciar el Cluster de Hadoop .........#
start-dfs.sh

# Iniciar el node magager i els recursos manager:
start-yarn.sh

#Verificar que els serveis estan iniciats:
jps

Step 7: Access the Namenode:
http://localhost:9870


Step 8: Access the Hadoop Resource Manager:
http://localhost:8088

#.............. Fi iniciar Cluster Hadoop ..............#

# Activar Entorn
source myenv/bin/activate