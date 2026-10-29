import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class SolicitudesActivas {
    private static final ConcurrentHashMap<String, Solicitud> solicitudes = new ConcurrentHashMap<>();

    private static class Solicitud {
        final String idOperario;
        final String idEstacion;
        boolean iniciada;
        long ultimaActividad = System.currentTimeMillis();

        Solicitud(String op, String est) { idOperario = op; idEstacion = est; }
    }

    public record Expirada(String idSolicitud, String idOperario, String idEstacion, boolean iniciada) {}

    public static synchronized boolean guardar(String idSolicitud, String idOperario, String idEstacion) {
        if (solicitudes.containsKey(idSolicitud) || estacionOcupada(idEstacion)) return false;
        solicitudes.put(idSolicitud, new Solicitud(idOperario, idEstacion));
        return true;
    }

    public static String obtenerOperario(String idSolicitud) {
        Solicitud s = solicitudes.get(idSolicitud);
        return s == null ? null : s.idOperario;
    }

    public static boolean esValida(String idSolicitud, String idOperario, String idEstacion) {
        Solicitud s = solicitudes.get(idSolicitud);
        return s != null && s.idOperario.equals(idOperario) && s.idEstacion.equals(idEstacion);
    }

    public static boolean estacionOcupada(String idEstacion) {
        for (Solicitud s : solicitudes.values()) {
            if (s.idEstacion.equals(idEstacion)) return true;
        }
        return false;
    }

    public static synchronized void actividad(String idSolicitud, boolean iniciada) {
        Solicitud s = solicitudes.get(idSolicitud);
        if (s != null) {
            s.ultimaActividad = System.currentTimeMillis();
            if (iniciada) s.iniciada = true;
        }
    }

    // Libera solicitudes sin respuesta/progreso durante un periodo excesivo.
    public static synchronized List<Expirada> expirar(long inactividadMs) {
        long ahora = System.currentTimeMillis();
        List<Expirada> salida = new ArrayList<>();
        solicitudes.forEach((id, s) -> {
            if (ahora - s.ultimaActividad > inactividadMs) {
                salida.add(new Expirada(id, s.idOperario, s.idEstacion, s.iniciada));
                solicitudes.remove(id);
            }
        });
        return salida;
    }


    // La fuga puede llegar antes que FINALIZADO del Engine: cerrar solo una vez.
    public static synchronized Expirada extraerPorEstacion(String idEstacion) {
        for (var entrada : solicitudes.entrySet()) {
            Solicitud s = entrada.getValue();
            if (s.idEstacion.equals(idEstacion)) {
                solicitudes.remove(entrada.getKey());
                return new Expirada(entrada.getKey(), s.idOperario, s.idEstacion, s.iniciada);
            }
        }
        return null;
    }
    public static synchronized void eliminar(String idSolicitud) {
        solicitudes.remove(idSolicitud);
    }
}
