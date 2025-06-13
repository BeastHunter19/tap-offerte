FROM apache/spark:4.0.0

USER 0
RUN pip install google-genai

# Install OpenJDK 17
RUN apt-get update && apt-get install -y openjdk-17-jdk && \
    update-alternatives --install /usr/bin/java java /usr/lib/jvm/java-17-openjdk-amd64/bin/java 1 && \
    update-alternatives --install /usr/bin/javac javac /usr/lib/jvm/java-17-openjdk-amd64/bin/javac 1

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER spark