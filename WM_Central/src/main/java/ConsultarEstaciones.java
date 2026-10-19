
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ConsultarEstaciones {

    public static void main(String[] args) {

        // Consulta para obtener las estaciones registradas.
        String sql = "SELECT id, ubicacion FROM estaciones";

        // Abrimos la conexión y consultamos los datos.
        try (Connection conexion = BaseDatos.conectar();
             Statement sentencia = conexion.createStatement();
             ResultSet resultados = sentencia.executeQuery(sql)) {

            System.out.println("=== ESTACIONES REGISTRADAS ===");

            // Recorremos las filas devueltas por SQLite.
            while (resultados.next()) {

                String id = resultados.getString("id");
                String ubicacion = resultados.getString("ubicacion");

                System.out.println("ID: " + id + " | Ubicacion: " + ubicacion);
            }

        } catch (SQLException e) {
            System.err.println("Error al consultar SQLite: " + e.getMessage());
        }
    }
}
