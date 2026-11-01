FROM maven:3.10-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml ./
COPY src ./src
RUN mvn -B -ntp -DskipTests package dependency:copy-dependencies -DoutputDirectory=target/dependency

FROM eclipse-temurin:17-jre
WORKDIR /data
COPY --from=build /build/target/wm-central-1.0-SNAPSHOT.jar /app/app.jar
COPY --from=build /build/target/dependency/ /app/lib/
EXPOSE 6000
ENTRYPOINT ["java", "-cp", "/app/app.jar:/app/lib/*"]
CMD ["WM_Central", "6000", "broker:29092"]
