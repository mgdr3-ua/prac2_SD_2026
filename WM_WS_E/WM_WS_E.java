import java.net.Socket;
import java.io.IOException;

public class WM_WS_E{

    public static void main(String[] args){

        System.out.println("=== WATER MANAGER ===");
        System.out.println("MÓDULO: Engine de estación (WM_WS_E)");

        //Datos de conexión del monitor
        String ipMonitor = "localhost";
        int puertoMonitor = 5001;

        //Intentamos establecer una conexión TCP
        try(Socket socket = new Socket(ipMonitor,puertoMonitor)){

            System.out.println("Conexión establecida con WM_WS_M.");
            System.out.println("Monitor: " + ipMonitor + ":" + puertoMonitor);

        }catch (IOException e){

            System.out.println("ERROR al conectar con WM_WS_M.");
            System.out.println("Motivo: " + e.getMessage());
        }
    }
}