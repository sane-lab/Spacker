cd flink-runtime
mvn clean install -DskipTests -Dcheckstyle.skip -Drat.skip=true
cd ../flink-streaming-java
mvn clean install -DskipTests -Dcheckstyle.skip -Drat.skip=true
