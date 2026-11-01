# ETAPA 1: Construcción de la aplicación con Maven y Java 17.
FROM maven:3.10-eclipse-temurin-17 AS build

# Establece el directorio de trabajo para la compilación.
WORKDIR /build

# Copia la configuración de Maven y el código fuente al contenedor.
COPY pom.xml ./
COPY src ./src

# Compila el proyecto sin ejecutar los tests y copia las dependencias necesarias.
RUN mvn -B -ntp -DskipTests package dependency:copy-dependencies -DoutputDirectory=target/dependency

# ETAPA 2: Creación de una imagen más ligera para ejecutar CENTRAL.
FROM eclipse-temurin:17-jre

# Establece el directorio de trabajo, utilizado también para los datos persistentes.
WORKDIR /data

# Copia el archivo JAR generado y sus dependencias desde la etapa de compilación.
COPY --from=build /build/target/wm-central-1.0-SNAPSHOT.jar /app/app.jar
COPY --from=build /build/target/dependency/ /app/lib/

# Documenta el puerto TCP utilizado por CENTRAL para recibir conexiones.
EXPOSE 6000

# Define el comando de ejecución de Java y su classpath.
ENTRYPOINT ["java", "-cp", "/app/app.jar:/app/lib/*"]

# Inicia CENTRAL en el puerto 6000 y configura la conexión con Kafka.
# El nombre broker se resuelve dentro de la red de Docker Compose.
CMD ["WM_Central", "6000", "broker:29092"]
