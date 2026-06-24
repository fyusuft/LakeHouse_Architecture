FROM python:3.10-bullseye

WORKDIR /app

# Install Java
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean

# JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

COPY . .

RUN pip install -r requirements.txt

EXPOSE 8501