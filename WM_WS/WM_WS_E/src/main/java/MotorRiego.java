
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Implementa la lógica de funcionamiento del motor de riego de una estación.
 *
 * Gestiona el inicio, seguimiento y finalización de los riegos, así como
 * las paradas manuales, los bloqueos administrativos y las fugas.
 * Utiliza Kafka para publicar eventos y enviar solicitudes a CENTRAL.
 * La ejecución de cada riego se realiza en un hilo independiente para
 * permitir que el motor siga atendiendo órdenes durante su funcionamiento.
 */
public class MotorRiego implements AutoCloseable {
    private final String idEstacion;
    private final AtomicBoolean averia;
    private final KafkaProducer<String, String> productor;
    private final double caudal = 12.0; // litros por minuto, valor de simulacion

    // Estado compartido entre el hilo de riego y los hilos que gestionan las órdenes.
    // volatile permite que los cambios sean visibles entre los distintos hilos.
    private volatile boolean bloqueada;
    private volatile boolean regando;
    private volatile String causaParada;
    private volatile Thread hiloRiegoActual;
    private String solicitudActual;

    /**
     * Inicializa el motor de riego y configura su productor Kafka.
     *
     * Se utiliza confirmación de todos los réplicas disponibles (acks=all)
     * para solicitar una mayor garantía de recepción de los mensajes.
     */
    public MotorRiego(String broker, String idEstacion, AtomicBoolean averia) {
        this.idEstacion = idEstacion;
        this.averia = averia;
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        productor = new KafkaProducer<>(p);
    }

    /**
     * Publica un evento del motor en el tópico eventos-estaciones.
     * Los mensajes utilizan el identificador de la estación como clave.
     * Si Kafka devuelve un error, este se muestra por consola.
     */
    private void publicar(String mensaje) {
        productor.send(new ProducerRecord<>("eventos-estaciones", idEstacion, mensaje),
                (meta, error) -> {
                    if (error != null) System.err.println("[ENGINE] Error Kafka: " + error.getMessage());
                });
        System.out.println("[RIEGO] " + mensaje);
    }

    // Una peticion local NO abre directamente la valvula: CENTRAL debe autorizarla.

    /**
     * Envía una solicitud de riego a CENTRAL a través de Kafka.
     *
     * Genera un identificador único y publica la petición en el tópico
     * solicitudes-riego. La operación espera hasta 10 segundos a que
     * Kafka confirme el envío.
     */
    public void solicitarRiego(String idOperario, int duracion) throws Exception {
        if (idOperario.isBlank() || duracion <= 0) {
            throw new IllegalArgumentException("Operario o duracion incorrecta");
        }
        String id = "SOL-" + UUID.randomUUID().toString().substring(0, 8);
        String solicitud = "SOLICITAR#" + id + "#" + idOperario + "#" + idEstacion + "#" + duracion;
        productor.send(new ProducerRecord<>("solicitudes-riego", idOperario, solicitud))
                .get(10, TimeUnit.SECONDS);
        System.out.println("[ENGINE] Solicitud enviada a CENTRAL: " + solicitud);
    }

    /**
     * Inicia un riego cuando la estación se encuentra disponible.
     *
     * Antes de comenzar comprueba que no exista una fuga, que la estación
     * no esté bloqueada y que no haya otro riego en ejecución.
     * Si alguna condición falla, publica un evento de rechazo.
     */
    public synchronized void iniciar(String idSolicitud, String idOperario, int duracion) {
        String motivo = averia.get() ? "FUGA" : bloqueada ? "BLOQUEADA" :
                regando ? "OCUPADA" : duracion <= 0 ? "ERROR" : null;
        if (motivo != null) {
            publicar("RECHAZADO#" + idSolicitud + "#" + idEstacion + "#" + motivo);
            return;
        }
        regando = true;
        causaParada = null;
        solicitudActual = idSolicitud;

        // El riego se ejecuta en segundo plano para no bloquear el hilo principal.
        Thread hilo = new Thread(() -> ejecutarRiego(idSolicitud, idOperario, duracion),
                "Riego-" + idEstacion);
        hiloRiegoActual = hilo;
        hilo.start();
    }

    /**
     * Simula el riego durante el tiempo solicitado.
     * Cada segundo calcula el volumen consumido a partir del caudal
     * y publica un evento PROGRESO en Kafka.
     *
     * El riego puede finalizar por tiempo cumplido, fuga, bloqueo
     * o parada manual. En todos los casos publica FINALIZADO
     * y libera el estado de la solicitud.
     */
    private void ejecutarRiego(String solicitud, String operario, int duracion) {
        int segundos = 0;
        double volumen = 0;
        String motivo = "TIEMPO";
        publicar("INICIADO#" + solicitud + "#" + idEstacion + "#" + operario);
        try {
            while (segundos < duracion) {
                if (averia.get() || causaParada != null) break;
                Thread.sleep(1000);
                if (averia.get() || causaParada != null) break;
                segundos++;

                // Conversión del tiempo transcurrido a minutos para calcular los litros.
                volumen = caudal * segundos / 60.0;
                publicar(String.format(Locale.US, "PROGRESO#%s#%s#%.2f#%.2f#%d",
                        solicitud, idEstacion, caudal, volumen, segundos));
            }
        } catch (InterruptedException e) {
            // Interrupcion intencionada: parada/fuga/bloqueo inmediata.
        } finally {
            // Se determina el motivo de finalización y se notifica el resultado.
            if (averia.get()) motivo = "FUGA";
            else if (causaParada != null) motivo = causaParada;
            publicar(String.format(Locale.US, "FINALIZADO#%s#%s#%s#%s#%d#%.2f",
                    solicitud, idEstacion, operario, motivo, segundos, volumen));

            // Se restablece el estado del motor para permitir futuros riegos.
            synchronized (this) {
                regando = false;
                solicitudActual = null;
                causaParada = null;
                hiloRiegoActual = null;
            }
        }
    }

    /**
     * Solicita la detención del riego actualmente en ejecución.
     * Guarda el motivo de parada e interrumpe el hilo para evitar
     * esperar al siguiente segundo de simulación.
     */
    private synchronized void solicitarParada(String motivo) {
        if (!regando) return;
        // La fuga tiene prioridad; una parada por bloqueo no puede perderse por ACTIVAR.
        if (!"BLOQUEO".equals(causaParada)) causaParada = motivo;
        Thread hilo = hiloRiegoActual;
        if (hilo != null) hilo.interrupt();
    }

    /**
     * Detiene manualmente un riego si coincide con la solicitud activa.
     */
    public void parar(String idSolicitud) {
        synchronized (this) {
            if (regando && idSolicitud.equals(solicitudActual)) solicitarParada("MANUAL");
        }
    }

    /**
     * Solicita la parada manual del riego activo, si existe.
     * Muestra un mensaje informativo cuando no hay ningún riego en curso.
     */
    public void pararActual() {
        if (!regando) {
            System.out.println("[RIEGO] No hay riego activo.");
            return;
        }
        solicitarParada("MANUAL");
        System.out.println("[RIEGO] Parada manual solicitada.");
    }

    /**
     * Registra una fuga en la estación y solicita la parada del riego.
     * AtomicBoolean permite compartir el estado de avería de forma
     * segura entre los componentes que lo consultan.
     */
    public void notificarFuga() {
        averia.set(true);
        solicitarParada("FUGA");
    }

    /**
     * Bloquea administrativamente la estación.
     * Si existe un riego activo, solicita su interrupción y publica
     * el evento BLOQUEADA para informar del cambio de estado.
     */
    public synchronized void bloquear(String idAdministracion) {
        bloqueada = true;
        solicitarParada("BLOQUEO");
        publicar("BLOQUEADA#" + idAdministracion + "#" + idEstacion);
    }

    /**
     * Reactiva una estación bloqueada administrativamente.
     * No permite la activación mientras exista una fuga y publica
     * el evento ACTIVADA cuando se elimina el bloqueo.
     */
    public synchronized void activar(String idAdministracion) {
        if (averia.get()) {
            System.out.println("[ENGINE] No se puede activar durante una fuga.");
            return;
        }
        bloqueada = false;
        publicar("ACTIVADA#" + idAdministracion + "#" + idEstacion);
    }

    /**
     * Solicita la detención del riego activo, espera brevemente
     * a que termine su hilo y cierra el productor Kafka.
     */
    @Override
    public void close() {
        solicitarParada("MANUAL");
        Thread hilo = hiloRiegoActual;
        if (hilo != null && hilo != Thread.currentThread()) {
            try { hilo.join(2500); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        productor.close();
    }
}
