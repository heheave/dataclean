package javaclz.mysql;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by xiaoke on 17-6-5.
 */
public class MySQLAccessor {

    private static final Logger log = Logger.getLogger(MySQLAccessor.class);

    private static final String URL_FORMAT = "jdbc:mysql://%s:%d/%s";

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            log.error("Load mysql driver error", e);
        }
    }

    public static Connection getConnector(String host, int port, String dbname, String user, String passwd) {
        String url = String.format(URL_FORMAT, host, port, dbname);
        Connection conn = null;
        try {
             conn = DriverManager.getConnection(url, user, passwd);
        } catch (SQLException e) {
            log.warn("Get connection error", e);
        }
        return conn;
    }

    public static void closeConn(Connection conn, Statement stat) {
        if (stat != null) {
            try {
                stat.close();
            } catch (SQLException e) {
                log.warn("Close statement error", e);
            }
        }


        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.warn("Close connection error", e);
            }
        }
    }
}
