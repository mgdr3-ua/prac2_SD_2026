import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;

public class MenuCentral extends Thread {
    private final String kafka;

    public MenuCentral(String kafka) { this.kafka = kafka; }

    private List<String> destinos(Scanner teclado) throws SQLException {
        System.out.print("ID de estacion (o TODAS): ");
        String id = teclado.nextLine().trim();
        if (id.equalsIgnoreCase("TODAS") || id.equals("*")) {
            return BaseDatos.listarIdsEstaciones();
        }
        return id.isBlank() ? List.of() : List.of(id);
    }

    @Override
    public void run() {
        Scanner teclado = new Scanner(System.in);
        try (ProductorKafka productor = new ProductorKafka(kafka)) {
            while (true) {
                System.out.println("\n=== MENU CENTRAL ===");
                System.out.println("1. Consultar estaciones");
                System.out.println("2. Bloquear una o TODAS");
                System.out.println("3. Activar una o TODAS");
                System.out.println("4. Iniciar riego en una o TODAS");
                System.out.println("0. Salir del menu (CENTRAL sigue activa)");
                System.out.print("Opcion: ");
                if (!teclado.hasNextLine()) return;
                String opcion = teclado.nextLine().trim();
                try {
                    if (opcion.equals("0")) return;
                    if (opcion.equals("1")) {
                        ConsultarEstaciones.mostrarEstaciones();
                        continue;
                    }
                    if (!opcion.equals("2") && !opcion.equals("3") && !opcion.equals("4")) {
                        System.out.println("Opcion desconocida.");
                        continue;
                    }
                    List<String> estaciones = destinos(teclado);
                    if (estaciones.isEmpty()) {
                        System.out.println("No hay estaciones para esa operacion.");
                        continue;
                    }
                    int duracion = 0;
                    if (opcion.equals("4")) {
                        System.out.print("Duracion del riego (segundos): ");
                        try {
                            duracion = Integer.parseInt(teclado.nextLine().trim());
                            if (duracion <= 0) throw new NumberFormatException();
                        } catch (NumberFormatException e) {
                            System.out.println("Duracion incorrecta.");
                            continue;
                        }
                    }
                    for (String est : estaciones) {
                        if (opcion.equals("2")) {
                            if (BaseDatos.bloquearEstacion(est)) {
                                String orden = "BLOQUEAR#ADM-" + UUID.randomUUID().toString().substring(0, 8) + "#" + est;
                                productor.enviarOrdenEstacion(est, orden);
                                System.out.println("[CENTRAL] Bloqueo enviado: " + est);
                            } else System.out.println("No se puede bloquear: " + est);
                        } else if (opcion.equals("3")) {
                            if (BaseDatos.activarEstacion(est)) {
                                String orden = "ACTIVAR#ADM-" + UUID.randomUUID().toString().substring(0, 8) + "#" + est;
                                productor.enviarOrdenEstacion(est, orden);
                                System.out.println("[CENTRAL] Activacion enviada: " + est);
                            } else System.out.println("No se puede activar: " + est);
                        } else {
                            // Misma ruta de validacion que WM_FO (no saltar autorizacion).
                            if (BaseDatos.estacionDisponible(est)) {
                                String solicitud = "SOLICITAR#SOL-" + UUID.randomUUID().toString().substring(0, 8)
                                        + "#ADMIN#" + est + "#" + duracion;
                                productor.enviarSolicitudRiego("ADMIN", solicitud);
                                System.out.println("[CENTRAL] Solicitud administrativa: " + solicitud);
                            } else System.out.println("Estacion no disponible: " + est);
                        }
                    }
                } catch (SQLException e) {
                    System.err.println("Error SQLite: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("Error en menu CENTRAL: " + e.getMessage());
        }
    }
}
