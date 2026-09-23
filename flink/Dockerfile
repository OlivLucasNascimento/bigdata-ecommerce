FROM bigdata-ecommerce-flume:latest AS hadoop-libs

FROM flink:1.17-scala_2.12

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip && \
    ln -sf /usr/bin/python3 /usr/bin/python

RUN pip3 install apache-flink==1.17.1 happybase

COPY --from=hadoop-libs /opt/hadoop-3.2.1 /opt/hadoop-3.2.1

ENV HADOOP_HOME=/opt/hadoop-3.2.1
ENV HADOOP_CLASSPATH="${HADOOP_HOME}/etc/hadoop:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/hdfs:${HADOOP_HOME}/share/hadoop/hdfs/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/mapreduce/lib/*:${HADOOP_HOME}/share/hadoop/mapreduce/*:${HADOOP_HOME}/share/hadoop/yarn:${HADOOP_HOME}/share/hadoop/yarn/lib/*:${HADOOP_HOME}/share/hadoop/yarn/*"

WORKDIR /app