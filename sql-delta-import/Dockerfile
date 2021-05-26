FROM openjdk:8-jdk

COPY . /app

WORKDIR /app

RUN curl -fsL "https://github.com/sbt/sbt/releases/download/v1.5.1/sbt-1.5.1.tgz" | tar xfz - -C /usr/share && \
    chown -R root:root /usr/share/sbt && \
    chmod -R 755 /usr/share/sbt && \
    ln -s /usr/share/sbt/bin/sbt /usr/local/bin/sbt

RUN wget https://github.com/Yelp/dumb-init/releases/download/v1.2.5/dumb-init_1.2.5_amd64.deb && \
    dpkg -i dumb-init_*.deb && \
    apt-get update && \
    apt-get -y install awscli

RUN sbt clean assembly && \
    chmod +x /app/sql-delta-import/dockerconf/entrypoint.sh

ENTRYPOINT ["/usr/bin/dumb-init", "--single-child", "/app/sql-delta-import/dockerconf/entrypoint.sh"]
