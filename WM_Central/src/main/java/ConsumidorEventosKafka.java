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

        Properties propiedades = new Properties();

        // Direccion del broker Kafka
        propiedades.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, direccionKafka);

        // Grupo independiente para los eventos del motor
        propiedades.put(ConsumerConfig.GROUP_ID_CONFIG, "central-eventos");

        // Recibimos los mensajes como texto
        propiedades.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propiedades.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Durante las primeras pruebas escuchamos mensajes nuevos
        propiedades.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        try (KafkaConsumer<String, String> consumidor =
                new KafkaConsumer<>(propiedades)) {

            consumidor.subscribe(Collections.singletonList("eventos-estaciones"));

            while (!Thread.currentThread().isInterrupted()) {

                var mensajes = consumidor.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> mensaje : mensajes) {
                    System.out.println("[KAFKA] Evento del motor: " + mensaje.value());

                    String[] campos = mensaje.value().split("#", -1);

                    if (campos.length == 4 && campos[0].equals("INICIADO")) {
                        String idEstacion = campos[2];

                        try {
                            BaseDatos.actualizarEstado(idEstacion, "REGANDO");
                            System.out.println("[CENTRAL] Estacion " + idEstacion + " en estado REGANDO.");
                        } catch (Exception e) {
                            System.err.println("[CENTRAL] Error al actualizar estacion: " + e.getMessage());
                        }
                    
                    } else if (campos.length == 7 && campos[0].equals("FINALIZADO")) {

                        String idEstacion = campos[2];
                        String motivo = campos[4];

                        try {
                            String estadoActual = BaseDatos.obtenerEstadoEstacion(idEstacion);

                            if ("REGANDO".equals(estadoActual)) {
                                if ("FUGA".equals(motivo)) {
                                    BaseDatos.actualizarEstado(idEstacion, "FUGA");
                                } else {
                                    BaseDatos.actualizarEstado(idEstacion, "DISPONIBLE");
                                }

                                System.out.println("[CENTRAL] Riego finalizado en " + idEstacion);
                            }

                        } catch (Exception e) {
                            System.err.println("[CENTRAL] Error al finalizar riego: " + e.getMessage());
                        }
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("[KAFKA] Error al recibir eventos: " + e.getMessage());
        }
    }
}