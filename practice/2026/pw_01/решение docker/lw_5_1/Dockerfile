# Dockerfile для Hadoop Single Node Cluster
FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive
ENV HADOOP_VERSION=3.3.4
ENV JAVA_VERSION=11
ENV HADOOP_HOME=/opt/hadoop
ENV JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-amd64
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin:$PATH

# Установка необходимых пакетов
RUN apt-get update && apt-get install -y \
    openjdk-${JAVA_VERSION}-jdk \
    openssh-server \
    openssh-client \
    wget \
    curl \
    vim \
    python3 \
    python3-pip \
    netcat \
    && rm -rf /var/lib/apt/lists/*

# Создание пользователя hadoop
RUN useradd -m -s /bin/bash hadoop

# Скачивание и установка Hadoop
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz \
    && mv hadoop-${HADOOP_VERSION} /opt/hadoop \
    && rm hadoop-${HADOOP_VERSION}.tar.gz

# Копирование конфигурационных файлов
COPY hadoop/core-site.xml $HADOOP_HOME/etc/hadoop/
COPY hadoop/hdfs-site.xml $HADOOP_HOME/etc/hadoop/
COPY hadoop/yarn-site.xml $HADOOP_HOME/etc/hadoop/
COPY hadoop/mapred-site.xml $HADOOP_HOME/etc/hadoop/
COPY hadoop/log4j.properties $HADOOP_HOME/etc/hadoop/
COPY hadoop/workers $HADOOP_HOME/etc/hadoop/
COPY hadoop/capacity-scheduler.xml $HADOOP_HOME/etc/hadoop/

# Исправить окончания строк в workers (удалить CRLF)
RUN sed -i 's/\r$//' $HADOOP_HOME/etc/hadoop/workers

# Настройка окружения - исправляем hadoop-env.sh
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-amd64" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh \
    && echo "export JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-amd64" >> $HADOOP_HOME/etc/hadoop/yarn-env.sh \
    && echo "export JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-amd64" >> $HADOOP_HOME/etc/hadoop/mapred-env.sh \
    && echo "export HADOOP_HOME=/opt/hadoop" >> /etc/profile \
    && echo "export JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-amd64" >> /etc/profile \
    && echo "export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop" >> /etc/profile \
    && echo "export PATH=\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$JAVA_HOME/bin:\$PATH" >> /etc/profile \
    && echo "export JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-amd64" >> ~/.bashrc \
    && echo "export HADOOP_HOME=/opt/hadoop" >> ~/.bashrc \
    && echo "export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop" >> ~/.bashrc \
    && echo "export PATH=\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$JAVA_HOME/bin:\$PATH" >> ~/.bashrc

# Настройка SSH и переменных окружения для SSH
RUN mkdir -p ~/.ssh /root/.ssh \
    && rm -f ~/.ssh/id_rsa ~/.ssh/id_rsa.pub \
    && ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa \
    && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys \
    && chmod 0600 ~/.ssh/authorized_keys \
    && chmod 700 ~/.ssh \
    && echo "JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.ssh/environment \
    && echo "HADOOP_HOME=/opt/hadoop" >> ~/.ssh/environment \
    && echo "HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop" >> ~/.ssh/environment \
    && rm -f /root/.ssh/id_rsa /root/.ssh/id_rsa.pub \
    && ssh-keygen -t rsa -P '' -f /root/.ssh/id_rsa \
    && cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys \
    && chmod 0600 /root/.ssh/authorized_keys \
    && chmod 700 /root/.ssh \
    && echo "JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> /root/.ssh/environment \
    && echo "HADOOP_HOME=/opt/hadoop" >> /root/.ssh/environment \
    && echo "HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop" >> /root/.ssh/environment \
    && echo "PermitUserEnvironment yes" >> /etc/ssh/sshd_config

# Форматирование HDFS (будет выполнено при первом запуске)
RUN echo "#!/bin/bash" > /start-hadoop.sh \
    && echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> /start-hadoop.sh \
    && echo 'export HADOOP_HOME=/opt/hadoop' >> /start-hadoop.sh \
    && echo 'export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop' >> /start-hadoop.sh \
    && echo 'export HDFS_NAMENODE_USER=root' >> /start-hadoop.sh \
    && echo 'export HDFS_DATANODE_USER=root' >> /start-hadoop.sh \
    && echo 'export HDFS_SECONDARYNAMENODE_USER=root' >> /start-hadoop.sh \
    && echo 'export YARN_RESOURCEMANAGER_USER=root' >> /start-hadoop.sh \
    && echo 'export YARN_NODEMANAGER_USER=root' >> /start-hadoop.sh \
    && echo 'export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH' >> /start-hadoop.sh \
    && echo '' >> /start-hadoop.sh \
    && echo 'if [ ! -d "/opt/hadoop_data/namenode/current" ]; then' >> /start-hadoop.sh \
    && echo '    echo "Formatting NameNode..."' >> /start-hadoop.sh \
    && echo '    $HADOOP_HOME/bin/hdfs namenode -format -force' >> /start-hadoop.sh \
    && echo 'fi' >> /start-hadoop.sh \
    && echo '' >> /start-hadoop.sh \
    && echo 'echo "Starting SSH..."' >> /start-hadoop.sh \
    && echo 'service ssh start' >> /start-hadoop.sh \
    && echo '' >> /start-hadoop.sh \
    && echo 'echo "Starting HDFS..."' >> /start-hadoop.sh \
    && echo '$HADOOP_HOME/sbin/start-dfs.sh' >> /start-hadoop.sh \
    && echo 'sleep 3' >> /start-hadoop.sh \
    && echo '' >> /start-hadoop.sh \
    && echo 'echo "Starting YARN..."' >> /start-hadoop.sh \
    && echo '$HADOOP_HOME/sbin/start-yarn.sh' >> /start-hadoop.sh \
    && echo 'sleep 3' >> /start-hadoop.sh \
    && echo '' >> /start-hadoop.sh \
    && echo 'echo "Uploading data to HDFS..."' >> /start-hadoop.sh \
    && echo 'if [ -f "/opt/data/database.csv" ]; then' >> /start-hadoop.sh \
    && echo '    $HADOOP_HOME/bin/hdfs dfs -mkdir -p /data' >> /start-hadoop.sh \
    && echo '    $HADOOP_HOME/bin/hdfs dfs -put /opt/data/database.csv /data/database.csv' >> /start-hadoop.sh \
    && echo '    echo "Data uploaded to HDFS successfully"' >> /start-hadoop.sh \
    && echo 'else' >> /start-hadoop.sh \
    && echo '    echo "Warning: /opt/data/database.csv not found"' >> /start-hadoop.sh \
    && echo 'fi' >> /start-hadoop.sh \
    && echo '' >> /start-hadoop.sh \
    && echo 'echo "Hadoop started!"' >> /start-hadoop.sh \
    && echo 'jps' >> /start-hadoop.sh \
    && echo 'tail -f /dev/null' >> /start-hadoop.sh \
    && chmod +x /start-hadoop.sh


# Установка Python библиотек
RUN python3 -m pip install --upgrade pip && \
    pip3 install jupyter pandas matplotlib seaborn pyspark && \
    pip3 install jupyterlab jupyter-console

# Копирование скриптов и данных
COPY scripts/ /opt/scripts/
COPY database.csv /opt/data/
RUN chmod +x /opt/scripts/*.py /opt/scripts/*.sh

# Создание рабочих директорий
RUN mkdir -p /tmp/hadoop-hadoop \
    && mkdir -p /opt/hadoop/data \
    && mkdir -p /opt/hadoop/logs

EXPOSE 9870 8088 8888 50070 50075 9000

WORKDIR /opt

CMD ["/start-hadoop.sh"]

