FROM python:3.11.9

WORKDIR /spark-app

COPY . .

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk

RUN pip install -r requirements.txt

ENV JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-amd64

CMD ["python", "/spark-app/main.py"]
