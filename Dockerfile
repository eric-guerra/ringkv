FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q dependency:go-offline
COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/target/ringkv-1.0.0.jar /app/ringkv.jar
VOLUME /data
ENV RINGKV_DATA=/data
ENTRYPOINT ["java", "-jar", "/app/ringkv.jar"]
