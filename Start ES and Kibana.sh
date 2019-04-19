#! to open Kibana on localhost:5601
sudo systemctl start kibana.service
#! to close Kibana
#! sudo systemctl stop kibana.service
#! to open ElasticSearch on localhost:9200
sudo docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.0.0

