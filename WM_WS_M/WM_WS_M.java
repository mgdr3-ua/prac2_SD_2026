import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class WM_WS_M {
    private static String ipCentral;
    private static int puertoCentral;
    private static String idEstacion;

    // Cada peticion al servidor de CENTRAL utiliza una conexion corta.
    private static boolean notificarCentral(String mensaje) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(ipCentral, puertoCentral), 3000);
            socket.setSoTimeout(3000);
            DataOutputStream salida = new DataOutputStream(socket.getOutputStream());
            DataInputStream entrada = new DataInputStream(socket.getInputStream());
            salida.writeUTF(mensaje);
            salida.flush();
            String respuesta = entrada.readUTF();
            System.out.println("[CENTRAL] " + mensaje + " -> " + respuesta);
            return respuesta.startsWith("STATUS#OK#");
        } catch (IOException e) {
            System.err.println("[MONITOR] CENTRAL no disponible: " + e.getMessage());
            return false;
        }
    }

    public static void main(String[] args) {
        if (args.length != 4 && args.length != 5) {
            System.out.println("Uso: java WM_WS_M <puerto_monitor> <ip_central> <puerto_central> <id_estacion> [ubicacion]");
            return;
        }

        int puertoMonitor;
        try {
            puertoMonitor = Integer.parseInt(args[0]);
            puertoCentral = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.err.println("Los puertos deben ser numeros.");
            return;
        }
        ipCentral = args[1];
        idEstacion = args[3];
        String ubicacion = args.length == 5 ? args[4] : "River Park";

        if (!notificarCentral("REGISTRO#" + idEstacion + "#" + ubicacion)) {
            System.err.println("Registro de la estacion rechazado. No se inicia el monitor.");
            return;
        }

        // Hasta recibir OK del Engine, CENTRAL no debe considerar la estacion disponible.
        notificarCentral("ESTADO#" + idEstacion + "#DESCONECTADA");

        try (ServerSocket servidor = new ServerSocket(puertoMonitor)) {
            System.out.println("[MONITOR] " + idEstacion + " escuchando Engine en " + puertoMonitor);
            boolean incidenciaNotificada = false;
            while (!Thread.currentThread().isInterrupted()) {
                System.out.println("[MONITOR] Esperando conexion o reconexion del Engine...");
                try (Socket engine = servidor.accept()) {
                    engine.setSoTimeout(10000); // El primer arranque Kafka puede tardar unos segundos.
                    DataInputStream entrada = new DataInputStream(engine.getInputStream());
                    DataOutputStream salida = new DataOutputStream(engine.getOutputStream());

                    // La identidad procede del Monitor, que ya se ha registrado en CENTRAL.
                    salida.writeUTF("ID#" + idEstacion);
                    salida.flush();
                    System.out.println("[MONITOR] Engine conectado: " + engine.getRemoteSocketAddress());
                    boolean primerOK = true;

                    while (true) {
                        salida.writeUTF("PING");
                        salida.flush();
                        String respuesta = entrada.readUTF();
                        engine.setSoTimeout(3000);
                        if ("OK".equals(respuesta)) {
                            if (incidenciaNotificada) {
                                if (notificarCentral("AVERIA#" + idEstacion + "#RESUELTA")) {
                                    incidenciaNotificada = false;
                                }
                            } else if (primerOK) {
                                notificarCentral("ESTADO#" + idEstacion + "#DISPONIBLE");
                            }
                            primerOK = false;
                            System.out.println("[MONITOR] " + idEstacion + " -> SALUDABLE");
                        } else {
                            if (!incidenciaNotificada) {
                                incidenciaNotificada = notificarCentral("AVERIA#" + idEstacion + "#FUGA");
                            }
                            System.out.println("[MONITOR] ALARMA en " + idEstacion + ": " + respuesta);
                        }
                        Thread.sleep(1000);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    // Incluye cierre de socket y timeout de respuesta: se trata como incidencia.
                    System.err.println("[MONITOR] Engine desconectado/sin respuesta: " + e.getMessage());
                    if (!incidenciaNotificada) {
                        incidenciaNotificada = notificarCentral("AVERIA#" + idEstacion + "#FUGA");
                    }
                    // No salir: aceptar el siguiente Engine en el mismo puerto.
                }
            }
        } catch (IOException e) {
            System.err.println("[MONITOR] No se pudo abrir el servidor: " + e.getMessage());
        }
    }
}
