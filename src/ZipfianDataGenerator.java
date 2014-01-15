/**
 * Created by ap349 on 12/10/13.
 */
import com.yahoo.ycsb.generator.ZipfianGenerator;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OraclePreparedStatement;

import java.sql.DriverManager;

public class ZipfianDataGenerator {
    private ZipfianGenerator zipfDistribution;
    private long             numberOfElements;
    private OracleConnection oracleConnection;
    private String           jdbcConnectionString;
    private String           username;
    private String           password;
    private String           tablename;
    private String           insertStatement;

    public ZipfianDataGenerator(long numberOfElements, double exponent) {
        this.zipfDistribution = new ZipfianGenerator(numberOfElements, exponent);
        this.numberOfElements = numberOfElements;
    }

    public String getJdbcConnectionString() {
        return jdbcConnectionString;
    }

    public void setJdbcConnectionString(String jdbcConnectionString) {
        this.jdbcConnectionString = jdbcConnectionString;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTablename() {
        return tablename;
    }

    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    public void initDatabase() {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            this.oracleConnection =
                    (OracleConnection) DriverManager.getConnection(jdbcConnectionString, username, password);

            this.insertStatement = "insert into " + this.tablename + " values (:1)";
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    public void populateData() {
        try {
            OraclePreparedStatement preparedStatement =
                    (OraclePreparedStatement) this.oracleConnection.prepareStatement(this.insertStatement);
            preparedStatement.setExecuteBatch(100);

            for (int i=0; i<this.numberOfElements; ++i) {
                preparedStatement.setLong(1, zipfDistribution.nextLong());
                preparedStatement.execute();
            }
            preparedStatement.close();
            oracleConnection.commit();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ZipfianDataGenerator zipfianDataGenerator = new ZipfianDataGenerator(Long.parseLong(args[0]), Double.parseDouble(args[1]));
        zipfianDataGenerator.setJdbcConnectionString("jdbc:oracle:thin:@ldap://oid.its.yale.edu:389/DB12,CN=OracleContext,dc=world");
        zipfianDataGenerator.setUsername("ap349");
        zipfianDataGenerator.setPassword("dba911");
        zipfianDataGenerator.setTablename("zipfian");
        zipfianDataGenerator.initDatabase();
        zipfianDataGenerator.populateData();
    }
}
