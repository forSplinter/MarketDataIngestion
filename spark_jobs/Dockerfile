FROM bitnami/spark:3.5.0

USER root
RUN apt-get update && apt-get install -y python3-pip

RUN pip install numpy pandas
RUN mkdir -p /opt/spark/jobs
WORKDIR  /opt/bitnami/spark

CMD [ "sleep", "infinity"]