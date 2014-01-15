import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import java.sql.*;

public class DataUniformZipfianGenerator
{
    public static void main(String[] args)
      throws Exception {

        String jdbcUrl =
             "jdbc:oracle:thin:@ldap://oid.its.yale.edu:389/HOP7,CN=OracleContext,dc=world";
        String user = "ap349";
        String password = "dba911";

        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
        connection.setAutoCommit(false);

        Statement statement = connection.createStatement();
        statement.execute("truncate table sales");

        String sql = "insert into sales\n" +
                "  ( year, amount )\n" +
                "   values\n" +
                "  ( add_months(to_date('01-JAN-1980','dd-mon-yyyy'), 12*(:1) ), :2 )";
        PreparedStatement ps = connection.prepareStatement(sql);

        // Using the data generator code from Yahoo labs at
        // https://github.com/brianfrankcooper/YCSB/tree/master/core/src/main/java/com/yahoo/ycsb/generator

        // UniformIntegerGenerator generates uniformly and linearly distributed integers between lowerBound
        // and higherBound inclusive.
        int lowerBound = Integer.parseInt("1");
        int higherBound = Integer.parseInt("30");
        UniformIntegerGenerator uniformIntegerGenerator = new UniformIntegerGenerator(lowerBound, higherBound);

        // Zipfian generator generates zipfian distributed integers between min and max with
        // zipfian constant zipfConstant.
        long min = 100;
        long max = 500;
        double zipfianConstant = 0.9;
        ZipfianGenerator zipfianGenerator = new ZipfianGenerator(min, max, zipfianConstant);

        for (int i=0; i<10000; ++i) {
            int random = uniformIntegerGenerator.nextInt();
            long randomz = zipfianGenerator.nextLong();
            ps.setInt(1, random);
            ps.setLong(2, randomz);
            ps.execute();
        }

        ps.close();
        connection.commit();
        connection.close();
    }
}
