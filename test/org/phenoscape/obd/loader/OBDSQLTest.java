package org.phenoscape.obd.loader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map.Entry;
import java.util.Properties;

import org.bbop.dataadapter.DataAdapterException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class OBDSQLTest {

    /** The db-host system property should contain the name of the database server. */
    public static final String DB_HOST = "db-host";
    /** The db-name system property should contain the name of the database. */
    public static final String DB_NAME = "db-name";
    /** The db-user system property should contain the database username. */
    public static final String DB_USER = "db-user";
    /** The db-password system property should contain the database password. */
    public static final String DB_PASSWORD = "db-password";

    private Connection connection;

    @BeforeClass
    public void initialize() throws DataAdapterException, SQLException, ClassNotFoundException, IOException {
        final Properties properties = new Properties();
        properties.load(OBDSQLTest.class.getResourceAsStream("connection.properties"));
        for (Entry<Object, Object> entry : properties.entrySet()) {
            System.setProperty(entry.getKey().toString(), entry.getValue().toString());
        }
        this.connection = this.createConnection();
    }

    @AfterClass
    public void destroy() throws SQLException {
        this.connection.close();
    }

    private Connection createConnection() throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        return DriverManager.getConnection("jdbc:postgresql://" + System.getProperty(DB_HOST) + "/" + System.getProperty(DB_NAME), System.getProperty(DB_USER), System.getProperty(DB_PASSWORD));
    }

    protected Connection getConnection() {
        return this.connection;
    }

}
