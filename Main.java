import java.sql.*;
import java.util.*;
import java.util.ArrayList;
import java.io.FileWriter;

public class Main {

    public static void main(String[] args) {
        String jdbcUrl = "jdbc:sap://54.147.66.43:30215";
        String username = "SAPABAP1";
        String password = "HvRS0ftWar3";
        
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        // Store composite keys
        List<List<Object>> pkList = new ArrayList<>();
        try {
            Class.forName("com.sap.db.jdbc.Driver");
            connection = DriverManager.getConnection(jdbcUrl, username, password);
            System.out.println("Connected to SAP HANA successfully.");

            processBucketDistribution(connection);
            return;
        } catch (SQLException e) {
            System.err.println("SQL Exception:");
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            System.err.println("JDBC Driver not found.");
            e.printStackTrace();
        }

    }

    public static void processBucketDistribution(Connection connection) throws SQLException {
   
            // Step 1: Fetch buckets
            long sampleStart = System.currentTimeMillis();
            String sampleQuery = """
                SELECT DOKVERSION, ID, LANGU, OBJECT, LINE, TYP, 'm' AS dummy_column
                FROM SAPABAP1.DOKTL
                TABLESAMPLE BERNOULLI(0.001)
                ORDER BY DOKVERSION, ID, LANGU, OBJECT, LINE, TYP
            """;
            Statement sampleStmt = connection.createStatement();
            ResultSet sampleRs = sampleStmt.executeQuery(sampleQuery);

            List<Doktl> doktlList = new ArrayList<>();
            while (sampleRs.next()) {
                Doktl doktl = new Doktl(
                    sampleRs.getString("DOKVERSION"),
                    sampleRs.getString("ID"),
                    sampleRs.getString("LANGU"),
                    sampleRs.getString("LINE"),
                    sampleRs.getString("OBJECT"),
                    sampleRs.getString("TYP")
                );
                doktlList.add(doktl);
            }
            long sampleEnd = System.currentTimeMillis();
            System.out.println("⏱️ Buketing query time: " + (sampleEnd - sampleStart) + " ms, Fetched " + doktlList.size() + " rows.");


            // Step 2: check PK distribution
            List<Integer> countValues = new ArrayList<>();
            int count = 1;
            for(Doktl currentDoktl : doktlList) {
                // get PK lower bound
                String dokVersion = currentDoktl.getDokVersion();
                String id = currentDoktl.getId();
                String langu = currentDoktl.getLangu();
                String line = currentDoktl.getLine();
                String object = currentDoktl.getObject();
                String typ = currentDoktl.getTyp();

                if (count >= doktlList.size()) {
                    System.out.println("Reached the end of the list.");
                    break;
                }

                Doktl nextDoktl = doktlList.get(count);
                count++;

                if (nextDoktl == null) {
                    break;
                }
                // get PK upper bound
                String nextDokVersion = nextDoktl.getDokVersion();
                String nextId = nextDoktl.getId();  
                String nextLangu = nextDoktl.getLangu();
                String nextLine = nextDoktl.getLine();
                String nextObject = nextDoktl.getObject();
                String nextTyp = nextDoktl.getTyp();


                // prepare the query to count rows in the range
                String query = """
                    SELECT COUNT(*)
                    FROM SAPABAP1.DOKTL
                    WHERE
                    (
                        DOKVERSION > ?
                        OR (DOKVERSION = ? AND ID > ?)
                        OR (DOKVERSION = ? AND ID = ? AND LANGU > ?)
                        OR (DOKVERSION = ? AND ID = ? AND LANGU = ? AND OBJECT > ?)
                        OR (DOKVERSION = ? AND ID = ? AND LANGU = ? AND OBJECT = ? AND LINE > ?)
                        OR (DOKVERSION = ? AND ID = ? AND LANGU = ? AND OBJECT = ? AND LINE = ? AND TYP >= ?)
                    )
                    AND
                    (
                        DOKVERSION < ?
                        OR (DOKVERSION = ? AND ID < ?)
                        OR (DOKVERSION = ? AND ID = ? AND LANGU < ?)
                        OR (DOKVERSION = ? AND ID = ? AND LANGU = ? AND OBJECT < ?)
                        OR (DOKVERSION = ? AND ID = ? AND LANGU = ? AND OBJECT = ? AND LINE < ?)
                        OR (DOKVERSION = ? AND ID = ? AND LANGU = ? AND OBJECT = ? AND LINE = ? AND TYP <= ?)
                    )
                """;
                PreparedStatement countStmt = connection.prepareStatement(query);
                // Set parameters for the prepared statement
                countStmt.setString(1, dokVersion);
                countStmt.setString(2, dokVersion);
                countStmt.setString(3, id);
                countStmt.setString(4, dokVersion);
                countStmt.setString(5, id);
                countStmt.setString(6, langu);
                countStmt.setString(7, dokVersion);
                countStmt.setString(8, id);
                countStmt.setString(9, langu);
                countStmt.setString(10, object);
                countStmt.setString(11, dokVersion);
                countStmt.setString(12, id);
                countStmt.setString(13, langu);
                countStmt.setString(14, object);
                countStmt.setString(15, line);
                countStmt.setString(16, dokVersion);
                countStmt.setString(17, id);
                countStmt.setString(18, langu);
                countStmt.setString(19, object);
                countStmt.setString(20, line);
                countStmt.setString(21, typ); 

                countStmt.setString(22, nextDokVersion);
                countStmt.setString(23, nextDokVersion);
                countStmt.setString(24, nextId);
                countStmt.setString(25, nextDokVersion);
                countStmt.setString(26, nextId);
                countStmt.setString(27, nextLangu);
                countStmt.setString(28, nextDokVersion);
                countStmt.setString(29, nextId);
                countStmt.setString(30, nextLangu);
                countStmt.setString(31, nextObject);
                countStmt.setString(32, nextDokVersion);
                countStmt.setString(33, nextId);
                countStmt.setString(34, nextLangu);
                countStmt.setString(35, nextObject);
                countStmt.setString(36, nextLine);
                countStmt.setString(37, nextDokVersion);
                countStmt.setString(38, nextId);
                countStmt.setString(39, nextLangu);
                countStmt.setString(40, nextObject);
                countStmt.setString(41, nextLine);
                countStmt.setString(42, nextTyp);

                ResultSet countRs = countStmt.executeQuery();
                if (countRs.next()) {
                    int countValue = countRs.getInt(1);
                    countValues.add(countValue);
                    System.out.println(countValue);
                }
 
            }

            // Step 3: aggregate and visualize the results

            try (FileWriter writer = new FileWriter("count_values.csv")) {
                for (Integer value : countValues) {
                    writer.write(value + "\n");
                }
                System.out.println("Exported count values to count_values.csv");
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }

