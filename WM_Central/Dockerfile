
# Compilamos CENTRAL utilizando Maven y Java 17
FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /app

COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src ./src
RUN mvn package -DskipTests -B dependency:copy-dependencies

# Imagen final con Java 17
FROM eclipse-temurin:17-jre

WORKDIR /app

COPY --from=build /app/target/classes ./classes
COPY --from=build /app/target/dependency ./lib

# Puerto TCP utilizado por CENTRAL
EXPOSE 5000

# Kafka se configurará al iniciar el contenedor
ENV KAFKA_BOOTSTRAP=localhost:9092

CMD ["sh", "-c", "java -cp 'classes:lib/*' WM_Central 5000 \"$KAFKA_BOOTSTRAP\""]
