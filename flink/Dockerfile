FROM flink:1.17-scala_2.12

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip && \
    ln -sf /usr/bin/python3 /usr/bin/python

RUN pip3 install apache-flink==1.17.1 happybase

WORKDIR /app