import java.net.Socket;

public class HiloCentral extends Thread {

    private Socket cliente;

    public HiloCentral(Socket cliente) {
        this.cliente = cliente;
    }

@Override
public void run() {

    try {
        // Mostramos qué cliente está siendo atendido por este hilo
        System.out.println("Hilo iniciado para el cliente: "
                + cliente.getInetAddress()
                + ":"
                + cliente.getPort());

        // Más adelante procesaremos aquí los mensajes del cliente

    } finally {
        try {
            // Cerramos la conexión cuando el hilo termina
            cliente.close();
        } catch (Exception e) {
            System.out.println("Error al cerrar la conexion con el cliente.");
        }
    }
}
}