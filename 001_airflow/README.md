# Pull the Docker image

`docker pull adrianw424/airflow:latest`
<br>

# Run the container

`docker run -dit --name airflow -p 8080:8080 --net bigdatanet adrianw424/airflow:latest`