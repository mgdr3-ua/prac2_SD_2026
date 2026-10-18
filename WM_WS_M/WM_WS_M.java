
import java.net.Socket;
import java.io.IOException;

public class WM_WS_M{

    public static void main(String[] args){ 

        System.out.println("--- WATER MANAGEMENT --- ");
        System.out.println("Módulo: Monitor de estación (WM_WS_M)");

        //Comprobamos los parámetros de entrada:
        if(args.length != 4){
            System.out.println("Uso: java WM_WS_M <puerto_monitor> <ip_central> <puerto_central> <id_estacion>");
            return;
        }

        //Guardamos los parámetros:
        int puertoMonitor = Integer.parseInt(args[0]);
        String ipCentral = args[1];
        int puertoCentral = Integer.parseInt(args[2]);
        String idEstacion = args[3];

        // Mostramos la confiuración:
        System.out.println("ID Eestación: " + idEstacion);
        System.out.println("Puerto monitor: " + puertoMonitor);
        System.out.println("IP central: " + ipCentral);
        System.out.println("Puerto central: " + puertoCentral);    

        System.out.println("Monitor iniciado correctamente!! ");

        //Intentamos conectar WM_Central
        try(Socket socket = new Socket(ipCentral, puertoCentral)){
            
            System.out.println("Conexion establecida con WM_Central.");
            System.out.println("Servidor: " + ipCentral + ":" + puertoCentral);

        } catch (IOException e){

            System.out.println("ERROR: No se puede conectar con WM_Central.");
            System.out.println("Motivo: " + e.getMessage());
        }
    }
}