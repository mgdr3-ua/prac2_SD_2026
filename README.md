# WaterManagement — Sistemas Distribuidos 2026/2027

Proyecto desarrollado para la asignatura **Sistemas Distribuidos** de la Universidad de Alicante.

## Descripción

WaterManagement es un sistema distribuido de gestión y supervisión de estaciones de riego. Permite solicitar y ejecutar riegos, consultar estaciones disponibles, controlar el consumo de agua y gestionar incidencias como fugas, bloqueos y desconexiones.

El sistema admite varios operarios y estaciones funcionando simultáneamente.

## Componentes

| Componente | Descripción |
|---|---|
| **WM_Central** | Coordina solicitudes, supervisa estaciones y gestiona la base de datos SQLite. |
| **WM_WS_M** | Monitoriza el estado de cada estación mediante sockets TCP. |
| **WM_WS_E** | Simula el funcionamiento del motor, el caudal y el consumo de agua. |
| **WM_FO** | Permite a los operarios solicitar riegos y recibir notificaciones. |

## Tecnologías utilizadas

- Java 17 y Apache Maven.
- Sockets TCP para la comunicación y supervisión directa.
- Apache Kafka 3.9.1 para el intercambio asíncrono de mensajes.
- SQLite para el almacenamiento de información.
- Docker y Docker Compose para la contenedorización.
- Git y GitHub para el control de versiones.

## Ejecución local con Docker

Con Docker Desktop iniciado, situarse en la raíz del proyecto y ejecutar:

**1. Arrancar los servicios:**

`docker compose up -d`

**2. Comprobar los contenedores:**

`docker compose ps`

**3. Ejecutar los operarios en terminales independientes:**

- OP-01: `docker compose run --rm --no-deps fo-01`
- OP-02: `docker compose run --rm --no-deps fo-02`

**4. Detener los servicios:**

`docker compose down`

Para obtener información detallada sobre la configuración, las pruebas y el despliegue distribuido, consultar la **memoria de la práctica** y el archivo `README_DOCKER.md`.

## Uso de inteligencia artificial

Durante el desarrollo se ha utilizado ChatGPT como herramienta de apoyo para la comprensión de conceptos, revisión del código, resolución de incidencias y preparación de la documentación.

**Ejemplos de prompts para documentar el uso de IA:**
1. Desarrollo de la comunicación mediante sockets TCP

Estoy desarrollando WaterManagement en Java y necesito implementar la comunicación entre WM_Central y WM_WS_M mediante sockets TCP. Partiendo de un servidor concurrente y sus hilos de atención, explícame cómo organizar el registro de estaciones, el intercambio de mensajes y la gestión de varias conexiones simultáneas. Indica también cómo detectar desconexiones y permitir que los monitores vuelvan a conectarse.

2. Integración de Apache Kafka entre los componentes

Analiza la arquitectura de WaterManagement y ayúdame a integrar Apache Kafka con WM_Central, WM_WS_E y WM_FO. Necesito que los operarios puedan solicitar riegos, que CENTRAL valide las peticiones y envíe órdenes a las estaciones, y que los motores publiquen eventos de progreso y finalización. Explica cómo organizar los topics, productores y consumidores, evitando que las notificaciones lleguen a operarios incorrectos.

3. Revisión del código y gestión de incidencias

Te proporcionaré un ZIP con el código completo de WaterManagement. Revisa la implementación de los cuatro módulos Java y comprueba especialmente la gestión de solicitudes simultáneas, los estados de las estaciones y la comunicación entre componentes. Identifica posibles errores relacionados con fugas, bloqueos, paradas manuales o desconexiones, y propone correcciones que mantengan la arquitectura existente.

4. Dockerización y resolución de errores

Tenemos funcionando localmente los cuatro módulos de WaterManagement y queremos ejecutarlos mediante Docker. Explícame cómo crear los Dockerfiles y configurar Docker Compose para arrancar CENTRAL, Kafka, dos monitores y dos motores. Después, ayúdame a comprobar las conexiones entre contenedores y diagnosticar errores de arranque, como los problemas de permisos que aparecen al iniciar el broker Kafka.

5. Diseño y ejecución de pruebas funcionales

Hemos terminado la implementación de WaterManagement y necesitamos realizar pruebas funcionales para comprobar que todos los componentes trabajan correctamente. Prepara un procedimiento paso a paso para verificar tres situaciones: un riego normal de 10 segundos, la detección de una fuga durante un riego y la ejecución simultánea de dos solicitudes por diferentes operarios. Indica los comandos, resultados esperados y evidencias que debemos recoger para la memoria.

La implementación y el funcionamiento final del sistema han sido comprobados mediante pruebas funcionales.

## Documentación

La memoria de la práctica contiene la arquitectura del sistema, la descripción de los componentes, los protocolos de comunicación, las pruebas realizadas y la guía de despliegue.
