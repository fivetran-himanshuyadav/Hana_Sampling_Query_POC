import java.sql.*;
import java.util.*;
import java.util.ArrayList;
import java.io.FileWriter;

public class Main {

    public static void main(String[] args) {
        String jdbcUrl = "";
        String username = "";
        String password = "";
        
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
                SELECT * FROM (
                    SELECT DOKVERSION, ID, LANGU, OBJECT, LINE, TYP
                    FROM SAPABAP1.DOKTL
                    WHERE MOD("$rowid$", 100000) = 1
                ) ORDER BY OBJECT, LINE, DOKVERSION, ID, LANGU, TYP
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

                // OBJECT, LINE, DOKVERSION, ID, LANGU, TYP
                // prepare the query to count rows in the range
                String query = """
                    SELECT COUNT(*)
                    FROM SAPABAP1.DOKTL
                    WHERE
                    (
                        OBJECT > ?
                        OR (OBJECT = ? AND LINE > ?)
                        OR (OBJECT = ? AND LINE = ? AND DOKVERSION > ?)
                        OR (OBJECT = ? AND LINE = ? AND DOKVERSION = ? AND ID > ?)
                        OR (OBJECT = ? AND LINE = ? AND DOKVERSION = ? AND ID = ? AND LANGU > ?)
                        OR (OBJECT = ? AND LINE = ? AND DOKVERSION = ? AND ID = ? AND LANGU = ? AND TYP >= ?)
                    )
                    AND
                    (
                        OBJECT < ?
                        OR (OBJECT = ? AND LINE < ?)
                        OR (OBJECT = ? AND LINE = ? AND DOKVERSION < ?)
                        OR (OBJECT = ? AND LINE = ? AND DOKVERSION = ? AND ID < ?)
                        OR (OBJECT = ? AND LINE = ? AND DOKVERSION = ? AND ID = ? AND LANGU < ?)
                        OR (OBJECT = ? AND LINE = ? AND DOKVERSION = ? AND ID = ? AND LANGU = ? AND TYP <= ?)
                    )
                """;
                PreparedStatement countStmt = connection.prepareStatement(query);

                // Set parameters for the first composite condition (greater than or equal comparison)
                countStmt.setString(1, object);         // OBJECT > ?
                countStmt.setString(2, object);         // OBJECT = ?
                countStmt.setString(3, line);           // LINE > ?
                countStmt.setString(4, object);         // OBJECT = ?
                countStmt.setString(5, line);           // LINE = ?
                countStmt.setString(6, dokVersion);     // DOKVERSION > ?
                countStmt.setString(7, object);         // OBJECT = ?
                countStmt.setString(8, line);           // LINE = ?
                countStmt.setString(9, dokVersion);     // DOKVERSION = ?
                countStmt.setString(10, id);            // ID > ?
                countStmt.setString(11, object);        // OBJECT = ?
                countStmt.setString(12, line);          // LINE = ?
                countStmt.setString(13, dokVersion);    // DOKVERSION = ?
                countStmt.setString(14, id);            // ID = ?
                countStmt.setString(15, langu);         // LANGU > ?
                countStmt.setString(16, object);        // OBJECT = ?
                countStmt.setString(17, line);          // LINE = ?
                countStmt.setString(18, dokVersion);    // DOKVERSION = ?
                countStmt.setString(19, id);            // ID = ?
                countStmt.setString(20, langu);         // LANGU = ?
                countStmt.setString(21, typ);           // TYP >= ?

                // Set parameters for the second composite condition (less than or equal comparison)
                countStmt.setString(22, nextObject);        // OBJECT < ?
                countStmt.setString(23, nextObject);        // OBJECT = ?
                countStmt.setString(24, nextLine);          // LINE < ?
                countStmt.setString(25, nextObject);        // OBJECT = ?
                countStmt.setString(26, nextLine);          // LINE = ?
                countStmt.setString(27, nextDokVersion);    // DOKVERSION < ?
                countStmt.setString(28, nextObject);        // OBJECT = ?
                countStmt.setString(29, nextLine);          // LINE = ?
                countStmt.setString(30, nextDokVersion);    // DOKVERSION = ?
                countStmt.setString(31, nextId);            // ID < ?
                countStmt.setString(32, nextObject);        // OBJECT = ?
                countStmt.setString(33, nextLine);          // LINE = ?
                countStmt.setString(34, nextDokVersion);    // DOKVERSION = ?
                countStmt.setString(35, nextId);            // ID = ?
                countStmt.setString(36, nextLangu);         // LANGU < ?
                countStmt.setString(37, nextObject);        // OBJECT = ?
                countStmt.setString(38, nextLine);          // LINE = ?
                countStmt.setString(39, nextDokVersion);    // DOKVERSION = ?
                countStmt.setString(40, nextId);            // ID = ?
                countStmt.setString(41, nextLangu);         // LANGU = ?
                countStmt.setString(42, nextTyp);           // TYP <= ?                

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
