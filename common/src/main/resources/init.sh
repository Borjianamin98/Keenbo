export ZOOKEEPER_HOST="slave-1"
export HBASE_TABLE="page"
export ELASTICSEARCH_NODE="localhost"
export ELASTICSEARCH_INDEX="search"
export KAFKA_TOPIC_LINKS="links"
export KAFKA_TOPIC_PAGES="pages"
declare -a hosts=("master" "slave-1" "slave-2" "slave-3")

# truncate hbase table
echo 'Truncating Hbase Table'
echo "truncate \"$HBASE_TABLE\"" | hbase shell -n >/dev/null
status=$?
if [ $status -ne 0 ]
then
	echo "Unable to truncate table page"
	exit
else
	echo 'HBase table Truncated'
fi

# initialize ElasticSearch index
echo 'DELETE Elasticsearch index'
curl -XDELETE "http://$ELASTICSEARCH_NODE:9200/$ELASTICSEARCH_INDEX" >/dev/null
sleep 1
echo 'Create Elasticsearch index'
curl -XPUT "http://$ELASTICSEARCH_NODE:9200/$ELASTICSEARCH_INDEX" -H 'Content-Type: application/json' -d'
{
    "settings" : {
        "index" : {
            "number_of_shards" : 6,
            "number_of_replicas" : 1
        }
    }
}' >/dev/null

status=$?
if [ $status -ne 0 ]
then
	echo "Unable to initialzie Elasticsearch"
	exit
else
	echo 'ElasticSearch initialized'
fi

# initialize kafka
echo 'Delete kafka topics'
/var/local/kafka/bin/kafka-topics.sh --delete --topic $KAFKA_TOPIC_LINKS --zookeeper localhost:2181
/var/local/kafka/bin/kafka-topics.sh --delete --topic $KAFKA_TOPIC_PAGES --zookeeper localhost:2181
sleep 3
echo 'Create kafka topics'
/var/local/kafka/bin/kafka-topics.sh --create --topic $KAFKA_TOPIC_LINKS --partitions 21 --replication-factor 2 --zookeeper $ZOOKEEPER_HOST:2181
/var/local/kafka/bin/kafka-topics.sh --create --topic $KAFKA_TOPIC_PAGES --partitions 21 --replication-factor 2 compression.type=gzip --zookeeper $ZOOKEEPER_HOST:2181

status=$?
if [ $status -ne 0 ]
then
	echo "Unable to initialzie kafka"
	exit
else
	echo "Kafka initialized successfully"
fi

# clear redis
echo 'Clear Redis history'
export mylist
for host in "${hosts[@]}"
do
   ssh -p 3031 root@$host 'redis-cli flushall'
done
echo 'Redis history cleared'
