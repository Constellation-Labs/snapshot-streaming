# syntax=docker/dockerfile:1.4
FROM amazoncorretto:11-alpine
ARG BUILD_VERSION

WORKDIR /app

RUN apk --no-cache add curl

RUN cat <<EOF > await.sh
#! /usr/bin/env sh
export CURL_RETRY_CMD="curl --retry 60 --retry-delay 1 --retry-all-errors --fail --silent"

eval "\$CURL_RETRY_CMD localstack:4510/_cluster/health"
EOF
RUN chmod +x await.sh

RUN cat <<EOF > start.sh
export JAVA_OPTS="\$JAVA_OPTS -Dcats.effect.tracing.mode=full -Dcats.effect.tracing.buffer.size=64"

if [ -n "\$CONFIG_FILE" ]
then
    export JAVA_OPTS="\$JAVA_OPTS -Dconfig.file=\$CONFIG_FILE"
fi

./await.sh && \
    java \$JAVA_OPTS -jar snapshot-streaming.jar
EOF
RUN chmod +x start.sh

COPY target/scala-2.13/cl-snapshot-streaming-assembly-${BUILD_VERSION}.jar snapshot-streaming.jar

CMD ["/bin/sh", "-c", "/app/start.sh"]
