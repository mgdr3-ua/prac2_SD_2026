FROM maven:3.10-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml ./
COPY src ./src
RUN mvn -B -ntp -DskipTests package dependency:copy-dependencies -DoutputDirectory=target/dependency

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/target/wm-ws-e-1.0-SNAPSHOT.jar /app/app.jar
COPY --from=build /build/target/dependency/ /app/lib/
ENTRYPOINT ["java", "-cp", "/app/app.jar:/app/lib/*", "WM_WS_E"]
CMD ["broker:29092", "monitor-ws01", "5001"]
