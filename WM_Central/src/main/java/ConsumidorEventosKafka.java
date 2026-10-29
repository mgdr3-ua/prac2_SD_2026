import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumidorEventosKafka extends Thread {
    private final String direccionKafka;

    public ConsumidorEventosKafka(String direccionKafka) {
        this.direccionKafka = direccionKafka;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, direccionKafka);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "central-eventos");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(props);
             ProductorKafka productor = new ProductorKafka(direccionKafka)) {
            consumidor.subscribe(Collections.singletonList("eventos-estaciones"));
            while (!Thread.currentThread().isInterrupted()) {
                var mensajes = consumidor.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> mensaje : mensajes) {
                    String contenido = mensaje.value();
                    System.out.println("[KAFKA] Evento del motor: " + contenido);
                    String[] c = contenido.split("#", -1);
                    try {
                        if (c.length == 4 && c[0].equals("INICIADO")) {
                            String sol = c[1], est = c[2], op = c[3];
                            if (!SolicitudesActivas.esValida(sol, op, est)) {
                                System.out.println("[CENTRAL] INICIADO ignorado: solicitud no valida.");
                                continue;
                            }
                            SolicitudesActivas.actividad(sol, true);
                            // Si CENTRAL ya bloqueo la estacion, no volver a REGANDO.
                            boolean iniciada = BaseDatos.cambiarEstadoSi(est, "DISPONIBLE", "REGANDO");
                            if (iniciada || "REGANDO".equals(BaseDatos.obtenerEstadoEstacion(est))) {
                                BaseDatos.iniciarDatosRiego(est, op);
                                productor.notificarOperario(op, "ACEPTADA#" + sol + "#" + op + "#" + est);
                            } else {
                                System.out.println("[CENTRAL] INICIADO en estacion no disponible: " + est);
                            }
                        } else if (c.length == 6 && c[0].equals("PROGRESO")) {
                            String sol = c[1], est = c[2];
                            String op = SolicitudesActivas.obtenerOperario(sol);
                            if (op != null && SolicitudesActivas.esValida(sol, op, est)
                                    && "REGANDO".equals(BaseDatos.obtenerEstadoEstacion(est))) {
                                SolicitudesActivas.actividad(sol, true);
                                BaseDatos.actualizarDatosRiego(est, Double.parseDouble(c[3]),
                                        Double.parseDouble(c[4]), Integer.parseInt(c[5]));
                                productor.notificarOperario(op, "PROGRESO#" + sol + "#" + op + "#" + est
                                        + "#" + c[3] + "#" + c[4] + "#" + c[5]);
                            }
                        } else if (c.length == 7 && c[0].equals("FINALIZADO")) {
                            String sol = c[1], est = c[2], op = c[3], motivo = c[4];
                            if (!SolicitudesActivas.esValida(sol, op, est)) {
                                System.out.println("[CENTRAL] FINALIZADO ignorado: solicitud no valida.");
                                continue;
                            }
                            // No sobrescribir bloqueos ni fugas previamente registradas.
                            if ("FUGA".equals(motivo)) {
                                BaseDatos.actualizarEstado(est, "FUGA");
                            } else if ("BLOQUEO".equals(motivo)) {
                                BaseDatos.bloquearEstacion(est);
                            } else {
                                BaseDatos.cambiarEstadoSi(est, "REGANDO", "DISPONIBLE");
                            }
                            productor.notificarOperario(op, "FINALIZADA#" + sol + "#" + op
                                    + "#" + est + "#" + motivo + "#" + c[5] + "#" + c[6]);
                            SolicitudesActivas.eliminar(sol);
                            BaseDatos.eliminarDatosRiego(est);
                            System.out.println("[CENTRAL] Riego finalizado en " + est + ", motivo " + motivo);
                        } else if (c.length == 4 && c[0].equals("RECHAZADO")) {
                            // RECHAZADO#solicitud#estacion#motivo
                            String sol = c[1], est = c[2];
                            String op = SolicitudesActivas.obtenerOperario(sol);
                            if (op == null || !SolicitudesActivas.esValida(sol, op, est)) continue;
                            productor.notificarOperario(op, "RECHAZADA#" + sol + "#" + op + "#" + est + "#" + c[3]);
                            SolicitudesActivas.eliminar(sol);
                            BaseDatos.eliminarDatosRiego(est);
                        } else if (c.length == 3 && (c[0].equals("BLOQUEADA") || c[0].equals("ACTIVADA"))) {
                            System.out.println("[CENTRAL] Confirmacion motor: " + contenido);
                        } else {
                            System.out.println("[CENTRAL] Evento no reconocido: " + contenido);
                        }
                    } catch (Exception e) {
                        System.err.println("[CENTRAL] Error procesando evento: " + e.getMessage());
                    }
                }
                if (!mensajes.isEmpty()) consumidor.commitSync();
            }
        } catch (Exception e) {
            System.err.println("[KAFKA] Error al recibir eventos: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
