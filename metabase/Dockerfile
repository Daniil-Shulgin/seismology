FROM openjdk:19-buster

ENV MB_PLUGINS_DIR=/app/plugins/
RUN mkdir -p ${MB_PLUGINS_DIR}
ADD https://downloads.metabase.com/v0.52.13/metabase.jar /app
ADD https://github.com/ClickHouse/metabase-clickhouse-driver/releases/download/1.53.4/clickhouse.metabase-driver.jar ${MB_PLUGINS_DIR}

RUN chmod 744 /app/plugins/clickhouse.metabase-driver.jar

CMD ["java", "-jar", "/app/metabase.jar"]