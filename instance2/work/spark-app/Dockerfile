FROM python:3.11.9

WORKDIR /spark-app

COPY . .

RUN apt-get update && \
    apt-get install -y make build-essential libssl-dev zlib1g-dev \
    libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
    libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev \
    libffi-dev liblzma-dev openjdk-17-jdk

RUN pip install -r requirements.txt

ENV JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-amd64

# CMD ["python", "/spark-app/src/test.py"]
