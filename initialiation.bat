docker-compose up -d
docker cp .\data namenode:/home
docker cp .\functions_docker.py namenode:/home/functions.py
docker cp .\queries_docker.py namenode:/home/queries.py
docker exec -it namenode apt-get update -y
docker exec -it namenode apt-get install -y python3-docker
docker exec -it namenode apt-get install -y python-pip
docker exec -it namenode pip install pandas
docker exec -it namenode pip install pydoop
docker exec -it namenode pip install future
docker exec -it namenode hdfs dfs -mkdir /home
docker exec -it namenode hdfs dfs -put /home/data/ /home
docker exec -it namenode python /home/queries.py
cmd /k 