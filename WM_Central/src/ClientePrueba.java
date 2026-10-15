import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class ClientePrueba {

    public static void main(String[] args) {

        // Conectamos con CENTRAL en nuestro ordenador
        try (Socket socket = new Socket("localhost", 5000)) {

            // Creamos el flujo para enviar mensajes
            DataOutputStream salida =
                    new DataOutputStream(socket.getOutputStream());

            // Enviamos un mensaje de registro de estación
            String mensaje = "REGISTRO#WS-04#River Park";
            salida.writeUTF(mensaje);
            salida.flush();

            System.out.println("Mensaje enviado: " + mensaje);

        } catch (IOException e) {
            System.out.println("Error de conexion: " + e.getMessage());
        }
    }
}

