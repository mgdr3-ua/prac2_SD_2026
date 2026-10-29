import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.sql.SQLException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumidorKafka extends Thread {

    private final String direccionKafka;

    public ConsumidorKafka(String direccionKafka) {
        this.direccionKafka = direccionKafka;
    }

    @Override
    public void run() {

        Properties propiedades = new Properties();

        // Direccion del broker Kafka
        propiedades.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, direccionKafka);

        // Grupo de consumidores de CENTRAL
        propiedades.put(ConsumerConfig.GROUP_ID_CONFIG, "central-solicitudes");

        // Los mensajes se reciben como texto
        propiedades.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propiedades.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Para esta primera prueba, leemos mensajes nuevos
        propiedades.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(propiedades);
                ProductorKafka productor = new ProductorKafka(direccionKafka)){

            consumidor.subscribe(Collections.singletonList("solicitudes-riego"));

            while (!Thread.currentThread().isInterrupted()) {

                var mensajes = consumidor.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> mensaje : mensajes) {
                    String solicitud = mensaje.value();

                    System.out.println("[KAFKA] Solicitud recibida: " + solicitud);

                    // Separamos los campos utilizando el protocolo acordado.
                    String[] campos = solicitud.split("#", -1);

                    // CONSULTAR#idConsulta#idOperario: listado solicitado por WM_FO.
                    if (campos.length == 3 && campos[0].equals("CONSULTAR")) {
                        try {
                            String listado = BaseDatos.operarioRegistrado(campos[2])
                                    ? BaseDatos.listarDisponibles() : "OPERARIO_NO_REGISTRADO";
                            productor.notificarOperario(campos[2],
                                    "LISTADO#" + campos[1] + "#" + campos[2] + "#" + listado);
                        } catch (SQLException e) {
                            System.err.println("[CENTRAL] Error consultando estaciones: " + e.getMessage());
                        }
                        continue;
                    }

                    // Una solicitud debe tener exactamente 5 campos.
                    if (campos.length != 5 || !campos[0].equals("SOLICITAR")) {
                        System.out.println("[KAFKA] Formato de solicitud incorrecto.");
                        continue;
                    }

                    String idSolicitud = campos[1];
                    String idOperario = campos[2];
                    String idEstacion = campos[3];

                    int duracion;

                    // Comprobamos que la duracion sea un numero positivo.
                    try {
                        duracion = Integer.parseInt(campos[4]);

                        if (duracion <= 0) {
                            System.out.println("[KAFKA] Duracion no valida.");
                            continue;
                        }
                    } catch (NumberFormatException e) {
                        System.out.println("[KAFKA] Duracion no valida.");
                        continue;
                    }

                    // Comprobamos que los identificadores no esten vacios.
                    if (idSolicitud.isBlank() || idOperario.isBlank() || idEstacion.isBlank()) {
                        System.out.println("[KAFKA] Faltan identificadores en la solicitud.");
                        continue;
                    }

                    // Informamos al operario de que hemos recibido su solicitud.
                    productor.notificarOperario(idOperario, "RECIBIDA#" + idSolicitud + "#" + idOperario + "#" + idEstacion);

                    // Indicamos que CENTRAL empieza a comprobar la solicitud.
                    productor.notificarOperario(idOperario,"VALIDANDO#" + idSolicitud + "#" + idOperario + "#" + idEstacion);
                    
                    // Consultamos SQLite para validar la solicitud.
                    try {
                        if (!BaseDatos.operarioRegistrado(idOperario)) {
                            System.out.println("[KAFKA] Solicitud " + idSolicitud + " rechazada: operario no registrado.");

                            String respuesta = "RECHAZADA#" + idSolicitud + "#" + idOperario + "#" + idEstacion + "#OPERARIO_INVALIDO";

                            productor.notificarOperario(idOperario, respuesta);
                            continue;
                        }

                        // Consultamos el estado real de la estacion.
                        String estado = BaseDatos.obtenerEstadoEstacion(idEstacion);

                        if (!"DISPONIBLE".equals(estado)) {

                            String motivo;

                            if (estado == null) {
                                motivo = "NO_REGISTRADA";
                            } else {
                                switch (estado) {
                                    case "FUERA_SERVICIO":
                                        motivo = "BLOQUEADA";
                                        break;
                                    case "REGANDO":
                                        motivo = "OCUPADA";
                                        break;
                                    case "FUGA":
                                        motivo = "FUGA";
                                        break;
                                    case "DESCONECTADA":
                                        motivo = "DESCONECTADA";
                                        break;
                                    default:
                                        motivo = "ERROR";
                                        break;
                                }
                            }

                            System.out.println("[KAFKA] Solicitud " + idSolicitud + " rechazada: " + motivo);

                            String respuesta = "RECHAZADA#" + idSolicitud + "#" + idOperario + "#" + idEstacion + "#" + motivo;

                            productor.notificarOperario(idOperario, respuesta);
                            continue;
                        }

                        System.out.println("[KAFKA] Solicitud " + idSolicitud + " validada correctamente.");
                        System.out.println("Estacion: " + idEstacion);
                        System.out.println("Operario: " + idOperario);
                        System.out.println("Duracion maxima: " + duracion + " segundos.");

                        // Enviamos al motor la orden de iniciar el riego.
                        String orden = "INICIAR#" + idSolicitud + "#" + idEstacion + "#" + idOperario + "#" + duracion;
                                                
                        // Intentamos reservar la estacion para esta solicitud
                        if (!SolicitudesActivas.guardar(idSolicitud, idOperario, idEstacion)) {

                            String motivo;

                            // Comprobamos por que no se ha podido guardar
                            if (SolicitudesActivas.obtenerOperario(idSolicitud) != null) {
                                motivo = "ERROR";
                                System.out.println("[CENTRAL] Solicitud duplicada: " + idSolicitud);
                            } else {
                                motivo = "OCUPADA";
                                System.out.println("[CENTRAL] Estacion ocupada: " + idEstacion);
                            }

                            String notificacion = "RECHAZADA#" + idSolicitud + "#" + idOperario
                                    + "#" + idEstacion + "#" + motivo;

                            productor.notificarOperario(idOperario, notificacion);
                            continue;
                        }

                        // Si es nueva, enviamos la orden al motor
                        productor.enviarOrdenEstacion(idEstacion, orden);
                    
                    } catch (SQLException e) {
                        System.err.println("[KAFKA] Error al consultar SQLite: " + e.getMessage());
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("Error en consumidor Kafka: " + e.getMessage());
        }
    }
}