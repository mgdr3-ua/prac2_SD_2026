import java.util.Scanner;

public class MenuCentral extends Thread {

    public void run() {

        Scanner teclado = new Scanner(System.in);
        int opcion = -1;

        // Mostramos el menú mientras CENTRAL esté funcionando.
        while (opcion != 0) {

            System.out.println("\n=== MENU CENTRAL ===");
            System.out.println("1. Consultar estaciones");
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
                // Mostramos las estaciones guardadas en SQLite.
                ConsultarEstaciones.mostrarEstaciones();           
            } else if (opcion == 0) {
                System.out.println("Menu cerrado.");
            } else {
                System.out.println("Opcion no valida.");
            }
        }
    }
}