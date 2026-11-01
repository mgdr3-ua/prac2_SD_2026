FROM eclipse-temurin:17-jdk AS build
WORKDIR /build
COPY WM_WS_M.java ./
RUN javac --release 17 WM_WS_M.java

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/WM_WS_M.class ./
EXPOSE 5001
ENTRYPOINT ["java", "-cp", "/app", "WM_WS_M"]
CMD ["5001", "central", "6000", "WS-01", "River Park"]
