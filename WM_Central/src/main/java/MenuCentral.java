import java.util.Scanner;

public class MenuCentral extends Thread {

    public void run() {

        Scanner teclado = new Scanner(System.in);
        int opcion = -1;

        // Mostramos el menú mientras CENTRAL esté funcionando.
        while (opcion != 0) {

            System.out.println("\n=== MENU CENTRAL ===");
            System.out.println("1. Consultar estaciones");
            System.out.println("2. Bloquear estacion");
            System.out.println("3. Activar estacion");
            System.out.println("0. Salir del menu");
            System.out.print("Selecciona una opcion: ");

            // Leemos la opción introducida por teclado.
            if (teclado.hasNextInt()) {
                opcion = teclado.nextInt();
            } else {
                teclado.next();
                System.out.println("Introduce un numero valido.");
                continue;
            }

            if (opcion == 1) {
                ConsultarEstaciones.mostrarEstaciones();

            } else if (opcion == 2) {

                // Pedimos el identificador de la estación que queremos bloquear.
                System.out.print("Introduce el ID de la estacion: ");
                String idEstacion = teclado.next();

                try {
                    if (BaseDatos.bloquearEstacion(idEstacion)) {
                        System.out.println("Estacion " + idEstacion + " bloqueada correctamente.");
                    } else {
                        System.out.println("No se puede bloquear la estacion.");
                    }
                } catch (java.sql.SQLException e) {
                    System.out.println("Error al bloquear la estacion: " + e.getMessage());
                }           
            } else if (opcion == 3) {

                // Pedimos el ID de la estación que queremos activar.
                System.out.print("Introduce el ID de la estacion: ");
                String idEstacion = teclado.next();

                try {
                    if (BaseDatos.activarEstacion(idEstacion)) {
                        System.out.println("Estacion " + idEstacion + " activada correctamente.");
                    } else {
                        System.out.println("No se puede activar la estacion.");
                    }
                } catch (java.sql.SQLException e) {
                    System.out.println("Error al activar la estacion: " + e.getMessage());
                }
            } else if (opcion == 0) {
                System.out.println("Menu cerrado.");
            } else {
                System.out.println("Opcion no valida.");
            }
        }
    }
}