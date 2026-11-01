# Classification d'offres d'emploi en temps réel (Kafka + Spark Streaming)

Classe chaque offre dans un domaine : Data, Développement, Sécurité ou Cloud.

## Architecture
jobs.txt → Producer Python → Topic Kafka "jobs" → Spark Structured Streaming → Console

## Lancement
1. `docker compose up -d`
2. Créer le topic :
   `docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic jobs --bootstrap-server localhost:9092`
3. `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 consumer.py`
4. `python producer.py`

## Niveaux réalisés
- [ ] Level 1 : classification par mots-clés
- [ ] Level 2 : MLlib
- [ ] Level 3 : Elasticsearch / Kibana