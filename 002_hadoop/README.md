# Pull the Docker image

`docker pull adrianw424/hadoop:latest`
<br>

# Run the container

`docker run -dit --name hadoop -p 8088:8088 -p 9870:9870 -p 9864:9864 -p 10000:10000 -p 8032:8032 -p 8030:8030 -p 8031:8031 -p 9000:9000 -p 8888:8888 --net bigdatanet adrianw424/hadoop:latest`