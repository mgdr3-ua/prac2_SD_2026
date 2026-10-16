import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class BaseDatos {

    // Dirección del archivo de la base de datos SQLite.
    private static final String URL = "jdbc:sqlite:wm_central.db";

    // Establece una conexión con SQLite.
    public static Connection conectar() throws SQLException {
        return DriverManager.getConnection(URL);
    }

    // Crea la tabla de estaciones si todavía no existe.
    public static void crearTablaEstaciones() throws SQLException {

        String sql = """
            CREATE TABLE IF NOT EXISTS estaciones (
                id TEXT PRIMARY KEY,
                ubicacion TEXT NOT NULL
            )
            """;

        // Abrimos la conexión y ejecutamos la sentencia SQL.
        // Los recursos se cierran automáticamente al terminar.
        try (Connection conexion = conectar();
             Statement sentencia = conexion.createStatement()) {

            sentencia.executeUpdate(sql);
            System.out.println("Tabla de estaciones preparada correctamente.");
        }
    }
}