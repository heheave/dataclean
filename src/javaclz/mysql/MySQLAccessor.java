package javaclz.mysql;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by xiaoke on 17-6-5.
 */
public class MySQLAccessor{

    private static final Logger log = LoggerFactory.getLogger(MySQLAccessor.class);

    private static final String URL_FORMAT = "jdbc:mysql://%s:%d/%s";

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            log.error("Load mysql driver error", e);
        }
    }

    public static ComboPooledDataSource getDataSource(String host, int port, String dbname, String user, String passwd) {
        ComboPooledDataSource ds = new ComboPooledDataSource();
        String url = String.format(URL_FORMAT, host, port, dbname);
        log.info("Mysql Connect Url is: " + url);
        ds.setJdbcUrl(url);
        ds.setUser(user);
        ds.setPassword(passwd);
        // use default deviceconfig
        try {
            ds.setDriverClass("com.mysql.jdbc.Driver");
        } catch (PropertyVetoException e) {
            log.error("Not sql driver", e);
            throw new RuntimeException("com.mysql.jdbc.Driver is not found");
        }
        ds.setInitialPoolSize(10);
        ds.setMinPoolSize(2);
        ds.setMaxIdleTime(1800);
        ds.setMaxPoolSize(20);
        ds.setAcquireIncrement(2);
        ds.setMaxStatements(100);
        return ds;
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
