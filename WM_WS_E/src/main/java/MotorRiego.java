import java.util.Locale;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class MotorRiego implements AutoCloseable {
    private final String idEstacion;
    private final AtomicBoolean averia;
    private final KafkaProducer<String, String> productor;
    private final double caudal = 12.0; // litros por minuto, valor de simulacion

    private volatile boolean bloqueada;
    private volatile boolean regando;
    private volatile String causaParada;
    private volatile Thread hiloRiegoActual;
    private String solicitudActual;

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

    private void publicar(String mensaje) {
        productor.send(new ProducerRecord<>("eventos-estaciones", idEstacion, mensaje),
                (meta, error) -> {
                    if (error != null) System.err.println("[ENGINE] Error Kafka: " + error.getMessage());
                });
        System.out.println("[RIEGO] " + mensaje);
    }

    // Una peticion local NO abre directamente la valvula: CENTRAL debe autorizarla.
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
        Thread hilo = new Thread(() -> ejecutarRiego(idSolicitud, idOperario, duracion),
                "Riego-" + idEstacion);
        hiloRiegoActual = hilo;
        hilo.start();
    }

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
                volumen = caudal * segundos / 60.0;
                publicar(String.format(Locale.US, "PROGRESO#%s#%s#%.2f#%.2f#%d",
                        solicitud, idEstacion, caudal, volumen, segundos));
            }
        } catch (InterruptedException e) {
            // Interrupcion intencionada: parada/fuga/bloqueo inmediata.
        } finally {
            if (averia.get()) motivo = "FUGA";
            else if (causaParada != null) motivo = causaParada;
            publicar(String.format(Locale.US, "FINALIZADO#%s#%s#%s#%s#%d#%.2f",
                    solicitud, idEstacion, operario, motivo, segundos, volumen));
            synchronized (this) {
                regando = false;
                solicitudActual = null;
                causaParada = null;
                hiloRiegoActual = null;
            }
        }
    }

    private synchronized void solicitarParada(String motivo) {
        if (!regando) return;
        // La fuga tiene prioridad; una parada por bloqueo no puede perderse por ACTIVAR.
        if (!"BLOQUEO".equals(causaParada)) causaParada = motivo;
        Thread hilo = hiloRiegoActual;
        if (hilo != null) hilo.interrupt();
    }

    public void parar(String idSolicitud) {
        synchronized (this) {
            if (regando && idSolicitud.equals(solicitudActual)) solicitarParada("MANUAL");
        }
    }

    public void pararActual() {
        if (!regando) {
            System.out.println("[RIEGO] No hay riego activo.");
            return;
        }
        solicitarParada("MANUAL");
        System.out.println("[RIEGO] Parada manual solicitada.");
    }

    public void notificarFuga() {
        averia.set(true);
        solicitarParada("FUGA");
    }

    public synchronized void bloquear(String idAdministracion) {
        bloqueada = true;
        solicitarParada("BLOQUEO");
        publicar("BLOQUEADA#" + idAdministracion + "#" + idEstacion);
    }

    public synchronized void activar(String idAdministracion) {
        if (averia.get()) {
            System.out.println("[ENGINE] No se puede activar durante una fuga.");
            return;
        }
        bloqueada = false;
        publicar("ACTIVADA#" + idAdministracion + "#" + idEstacion);
    }

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
