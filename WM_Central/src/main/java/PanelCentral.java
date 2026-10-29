import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;

// Panel de texto en una terminal independiente para no interferir con MenuCentral.
public class PanelCentral {
    private static final String RESET = "\u001B[0m";
    private static final String VERDE = "\u001B[42;30m";
    private static final String ROJO = "\u001B[41;37m";
    private static final String NARANJA = "\u001B[43;30m";
    private static final String GRIS = "\u001B[100;37m";
    private static final String PARPADEO = "\u001B[5;42;30m";

    public static void main(String[] args) throws Exception {
        System.out.println("Panel CENTRAL iniciado. Ctrl+C para salir.");
        // ANSI funciona en Windows Terminal/PowerShell actuales.
        while (!Thread.currentThread().isInterrupted()) {
            mostrar();
            Thread.sleep(1000);
        }
    }

    private static void mostrar() {
        System.out.print("\u001B[H\u001B[2J");
        System.out.flush();
        System.out.println("========== WATER MANAGEMENT | PANEL CENTRAL ==========");
        System.out.println("Actualizacion: cada segundo | Ctrl+C para cerrar");
        System.out.println("-------------------------------------------------------");
        String sql = "SELECT e.id,e.ubicacion,e.estado,r.operario,r.caudal,r.volumen,r.segundos "
                + "FROM estaciones e LEFT JOIN riegos_actuales r ON r.estacion=e.id ORDER BY e.id";
        try (Connection con = BaseDatos.conectar();
             Statement st = con.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            boolean alguna = false;
            while (rs.next()) {
                alguna = true;
                String estado = rs.getString("estado");
                String color = switch (estado) {
                    case "DISPONIBLE" -> VERDE;
                    case "REGANDO" -> PARPADEO;
                    case "FUGA" -> ROJO;
                    case "FUERA_SERVICIO" -> NARANJA;
                    default -> GRIS;
                };
                String etiqueta = "FUERA_SERVICIO".equals(estado) ? "Fuera de Servicio" : estado;
                System.out.println(color + " " + rs.getString("id") + " | " + etiqueta + " " + RESET);
                System.out.println("  Ubicacion: " + rs.getString("ubicacion"));
                if ("REGANDO".equals(estado)) {
                    String operario = rs.getString("operario");
                    System.out.println("  Operario: " + (operario == null ? "(pendiente)" : operario));
                    System.out.printf(Locale.US, "  Caudal: %.2f L/min | Volumen: %.2f L | Tiempo: %d s%n",
                            rs.getDouble("caudal"), rs.getDouble("volumen"), rs.getInt("segundos"));
                }
                System.out.println("-------------------------------------------------------");
            }
            if (!alguna) System.out.println("No hay estaciones registradas.");
        } catch (Exception e) {
            System.out.println("Esperando datos de CENTRAL: " + e.getMessage());
        }
    }
}
