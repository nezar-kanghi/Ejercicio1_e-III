import java.sql.*;
import java.util.*;

public class RegistrarEtapa {

    public static void main(String[] args) {

        String url = "jdbc:oracle:thin:@localhost:1521:xe";
        String usuario = "RIBERA";
        String password = "ribera";

        Scanner sc = new Scanner(System.in);

        try (Connection conexion = DriverManager.getConnection(url, usuario, password)) {

            // 1. OBTENER MAX + 1

            String sqlMax = "SELECT MAX(numero) FROM etapa";

            PreparedStatement psMax = conexion.prepareStatement(sqlMax);

            ResultSet rsMax = psMax.executeQuery();

            int numero = 1;

            rsMax.next();
            numero = rsMax.getInt(1) + 1;

            System.out.println("Numero de etapa generado: " + numero);


            // 2. DATOS POR CONSOLA


            System.out.print("Origen: ");
            String origen = sc.nextLine();

            System.out.print("Destino: ");
            String destino = sc.nextLine();

            System.out.print("Distancia km: ");
            int distancia = sc.nextInt();
            sc.nextLine();

            System.out.print("Fecha (YYYY-MM-DD): ");
            String fechaTexto = sc.nextLine();

            java.sql.Date fecha = java.sql.Date.valueOf(fechaTexto);

            // 3. INICIO TRANSACCION

            conexion.setAutoCommit(false);

            // INSERT ETAPA

            try {

                String sqlEtapa = "INSERT INTO etapa " +
                                "(numero, origen, destino, distancia_km, fecha) " +
                                "VALUES (?, ?, ?, ?, ?)";

                PreparedStatement psEtapa = conexion.prepareStatement(sqlEtapa);

                psEtapa.setInt(1, numero);
                psEtapa.setString(2, origen);
                psEtapa.setString(3, destino);
                psEtapa.setInt(4, distancia);
                psEtapa.setDate(5, fecha);

                psEtapa.executeUpdate();

                // BORRAR PARTICIPACIONES ANTERIORES

                String sqlDelete =
                        "DELETE FROM participacion WHERE numero_etapa = ?";

                PreparedStatement psDel = conexion.prepareStatement(sqlDelete);

                psDel.setInt(1, numero);
                psDel.executeUpdate();

                // OBTENER CICLISTAS (PREPAREDSTATEMENT)


                String sqlCiclistas = "SELECT id_ciclista FROM ciclista";

                PreparedStatement psCiclistas = conexion.prepareStatement(sqlCiclistas);

                ResultSet rs = psCiclistas.executeQuery();

                ArrayList<Integer> ciclistas = new ArrayList<>();

                while (rs.next()) {
                    ciclistas.add(rs.getInt(1));
                }

                //RANDOM DE CICLISTAS
                Collections.shuffle(ciclistas);

                // SOLO TOP 5

                int top = Math.min(5, ciclistas.size());

                ArrayList<Integer> posiciones = new ArrayList<>();

                for (int i = 1; i <= top; i++) {
                    posiciones.add(i);
                }

                Collections.shuffle(posiciones);

                // INSERT PARTICIPACION

                String sqlPart =
                        "INSERT INTO participacion " +
                        "(numero_etapa, id_ciclista, posicion, puntos) " +
                        "VALUES (?, ?, ?, ?)";

                PreparedStatement psPart = conexion.prepareStatement(sqlPart);

                for (int i = 0; i < top; i++) {

                    int id = ciclistas.get(i);
                    int posicion = posiciones.get(i);

                    int puntos = 0;

                    switch (posicion) {
                        case 1:
                            puntos = getPuntos();
                            break;
                        case 2: puntos = 90; break;
                        case 3: puntos = 80; break;
                        case 4: puntos = 70; break;
                        case 5: puntos = 60; break;
                    }

                    psPart.setInt(1, numero);
                    psPart.setInt(2, id);
                    psPart.setInt(3, posicion);
                    psPart.setInt(4, puntos);

                    psPart.executeUpdate();
                }


                // COMMIT


                conexion.commit();

                System.out.println("Etapa insertada correctamente");
                System.out.println("Numero: " + numero);
                System.out.println("Top ciclistas: " + top);
                System.out.println("Fecha: " + fecha);

            } catch (SQLException e) {

                conexion.rollback();

                System.out.println(
                        "Etapa cancelada por error. No se guardaron los datos.");

                System.out.println("ERROR REAL: " + e.getMessage());
            }

        } catch (SQLException e) {
            System.out.println("Error conexion: " + e.getMessage());
        }

        sc.close();
    }

    private static int getPuntos() {
        int puntos;
        puntos = 100;
        return puntos;
    }
}