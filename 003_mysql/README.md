# Pull the Docker image

`docker pull adrianw424/mysql:latest`
<br>

# Run the container

`docker run --name mysql -p 3306:3306 --net bigdatanet -e MYSQL_ROOT_PASSWORD=mysql -d adrianw424/mysql:latest`