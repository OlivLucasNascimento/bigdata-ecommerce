
FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

ENV FLUME_HOME=/opt/flume

RUN curl -fSL \
    https://archive.apache.org/dist/flume/1.11.0/apache-flume-1.11.0-bin.tar.gz \
    -o /tmp/flume.tar.gz \
    && tar -xzf /tmp/flume.tar.gz -C /opt \
    && mv /opt/apache-flume-1.11.0-bin /opt/flume \
    && rm /tmp/flume.tar.gz

    
   
    RUN rm -f /opt/flume/lib/guava-11.0.2.jar \
        && cp /opt/hadoop-3.2.1/share/hadoop/common/lib/guava-27.0-jre.jar /opt/flume/lib/

ENV PATH="${FLUME_HOME}/bin:${PATH}"

WORKDIR /app