FROM c.rzp.io/razorpay/onggi:maven-java8

RUN \
  curl -fsL "https://github.com/sbt/sbt/releases/download/v1.5.1/sbt-1.5.1.tgz" | tar xfz - -C /usr/share && \
  chown -R root:root /usr/share/sbt && \
  chmod -R 755 /usr/share/sbt && \
  ln -s /usr/share/sbt/bin/sbt /usr/local/bin/sbt

RUN sbt clean assembly

