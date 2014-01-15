import java.sql.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Stack;

import oracle.sql.ROWID;

/**
 * Created by ap349 on 12/17/13.
 */
public class SqlStatistics {
    public static String ORACLE_DRIVER_NAME    = "oracle.jdbc.driver.OracleDriver";
    public static String MSSQL_DRIVER_NAME     = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    public static String MYSQL_DRIVER_NAME     = "com.mysql.jdbc.Driver";
    public static String POSTGRESS_DRIVER_NAME = "org.postgresql.Driver";
    public static int TOPK_COUNT            =  2048;

    private String jdbcConnectionString;
    private String username;
    private String password;
    private Connection connection;

    public SqlStatistics(String jdbcConnectionString,
                         String username,
                         String password,
                         String driver) {
        this.jdbcConnectionString = jdbcConnectionString;
        this.username = username;
        this.password = password;

        try {
            Class.forName(driver);
            this.connection = DriverManager.getConnection(jdbcConnectionString, username, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException se) {
            se.printStackTrace();
        }
    }

    private String getErrorPercent(double actual, double estimate) {
        DecimalFormat decimalFormat = new DecimalFormat("##.00");
        return decimalFormat.format((Math.abs(actual - estimate)/actual)*100);
    }

    private void highFrequencyInclusionError(String sql, HashMap<String,
             ColumnStats> estimate, String tableName) throws SQLException {
        System.out.println("Accuracy report for TOP-K sketch:");
        System.out.println("---------------------------------");

        for ( String column : estimate.keySet() ) {
            int missedCount = 0;
            HashMap<String, Long> actualMap = estimate.get(column).getTopk().getTopKElementsHash();

            String countSql = " select value, cnt, min(cnt) over() mincnt \n" +
                              "   from ( select " + column + " as value, count(*) cnt\n" +
                              "            from " + tableName + " \n" +
                              "           group by " + column + " \n" +
                              "           order by 2 desc\n" +
                              "        )\n" +
                              "   where rownum < " + TOPK_COUNT;

            PreparedStatement ps = connection.prepareStatement(countSql);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                Object value = rs.getObject(1);
                long actualCount = rs.getLong(2);
                long estimateCount;
                long minCount = rs.getLong(3);

                System.out.println("Column: " + column + " Value: " + value.toString());

                if ( actualMap.containsKey(value.toString() ) ) {
                    estimateCount = actualMap.get(value.toString());
                    System.out.println(" Actual:    " + actualCount +
                                       " Estimate:  " + estimateCount +
                                       " Min Count: " + minCount +
                                       " Error(%):  " + getErrorPercent(actualCount, estimateCount));
                } else {
                    System.out.println("Actual: " + actualCount + " Estimate: Missing");
                    ++missedCount;
                }
            }

            System.out.println("Total missing: " + missedCount);

            rs.close();
            ps.close();
        }
    }

    private void calculateNDVError(String sql, HashMap<String, ColumnStats> estimate, String tableName)
        throws SQLException {

        StringBuffer ndvSql = new StringBuffer("select ");

        for ( String column : estimate.keySet() ) {
            if ( ndvSql.toString().equals("select ") ) {
                ndvSql.append("count(distinct " + column + ") as " + column + " ");
            } else {
                ndvSql.append(", count(distinct " + column + ") as " + column + " ");
            }
        }

        ndvSql.append(" from " + tableName);

        //System.out.println(ndvSql.toString());

        PreparedStatement preparedStatement = connection.prepareStatement(ndvSql.toString());
        ResultSet resultSet = preparedStatement.executeQuery();

        while ( resultSet.next() ) {
            for ( String column : estimate.keySet() ) {
                double count = resultSet.getLong(column);
                double estimateCount = estimate.get(column).getCardinalitySketch().estimateNDV();
                System.out.println(" Column: " + column +
                                   " Exact: " + count +
                                   " Estimate: " + estimateCount +
                                   " Error(%): " + getErrorPercent(count, estimateCount));
            }
        }
    }

    private void dumpCardinalitySketchIntoTopk(ColumnStats columnStats) {
        CardinalitySketch sketch = columnStats.getCardinalitySketch();
        TopK topK = columnStats.getTopk();
        PriorityQueue<CardinalitySketch.Node> pq = sketch.getFrequencies();
        CardinalitySketch.Node node = pq.poll();

        while ( node != null ) {
            topK.add(node.getValue(), (int)node.getFrequency(), node.getRowid());
            node = pq.poll();
        }
    }

    public void dumpCardinalitySketchIntoTopk(HashMap<String, ColumnStats> columnStats) {
        for (String columnLabel : columnStats.keySet() ) {
            if ( !columnStats.get(columnLabel).isDoingLossyCounting() ) {
               dumpCardinalitySketchIntoTopk(columnStats.get(columnLabel));
            }
        }
    }

    private void gatherColumnStats(ResultSet resultSet, HashMap<String, ColumnStats> columnStats )
            throws SQLException {
        while ( resultSet.next() ) {
            ROWID rowid = (ROWID) resultSet.getRowId(1);
            for (String columnLabel : columnStats.keySet() ) {
                Object object = resultSet.getObject(columnLabel);
                ColumnStats columnStat = columnStats.get(columnLabel);
                columnStat.incrementNumRows();

                if ( object == null ) {
                    columnStat.incrementNumNulls();
                }  else {
                    CardinalitySketch sketch = columnStats.get(columnLabel).getCardinalitySketch();
                    sketch.add(object.toString());

                    if ( columnStat.isDoingLossyCounting() ) {
                        TopK topk = columnStats.get(columnLabel).getTopk();
                        topk.add(object.toString(), 1, rowid);
                    }

                    if ( !columnStat.isDoingLossyCounting() && sketch.getSize() == sketch.getMaxelements() ) {
                        System.out.println("Switching to lossy counting for " + columnLabel);
                        dumpCardinalitySketchIntoTopk(columnStat);
                        columnStat.setDoingLossyCounting(true);
                    }
                }
            }
        }
    }

    public HashMap<String, ColumnStats> gatherSqlStats(String sql)
            throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setFetchSize(200);
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        HashMap<String, ColumnStats> statsHashMap = new HashMap<String, ColumnStats>();

        for ( int i=2; i<=columnCount; ++i) {
            String label = resultSetMetaData.getColumnLabel(i);
            String className = resultSetMetaData.getColumnClassName(i);
            ColumnStats columnStats = new ColumnStats();
            columnStats.setColumnName(label);
            columnStats.setColumnClassName(className);
            columnStats.setTopk(new TopK(TOPK_COUNT));
            columnStats.setDoingLossyCounting(false);
            columnStats.setCardinalitySketch(new CardinalitySketch());
            statsHashMap.put(label, columnStats);
        }

        gatherColumnStats(resultSet, statsHashMap);
        return statsHashMap;
    }

    private void printColumnStats(HashMap<String, ColumnStats> value) {
        for (String key : value.keySet() ) {
            ColumnStats columnStats = value.get(key);
            System.out.println("Column Name: " + key + " NDV: " + columnStats.getCardinalitySketch().estimateNDV());
            Stack<RowidMap> stack = columnStats.getTopk().getTopKElements();

            while ( !stack.empty() ) {
                RowidMap map = stack.pop();
                System.out.println(map.getObject().toString() + ": " + map.getCount());
            }
        }
    }

    public static void main(String[] args)
            throws SQLException {
        String jdbcstr = "jdbc:oracle:thin:@ldap://xxx.xxx.xxx.xxx:389/XXXX,CN=OracleContext,dc=world";
        String username = "xxxx";
        String password = "xxxx";
        String sql = "select a.rowid, a.sample,a.sample1 from zipfian a";

        SqlStatistics statistics = new SqlStatistics(jdbcstr, username, password, ORACLE_DRIVER_NAME);
        HashMap<String, ColumnStats> val = statistics.gatherSqlStats(sql);
        statistics.dumpCardinalitySketchIntoTopk(val);
        statistics.printColumnStats(val);
        statistics.calculateNDVError(sql, val, "zipfian");
        statistics.highFrequencyInclusionError(sql, val, "zipfian");
    }
}
