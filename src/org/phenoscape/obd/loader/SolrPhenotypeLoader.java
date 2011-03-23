package org.phenoscape.obd.loader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class SolrPhenotypeLoader {
    
    /** The db-host system property should contain the name of the database server. */
    public static final String DB_HOST = "db-host";
    /** The db-name system property should contain the name of the database. */
    public static final String DB_NAME = "db-name";
    /** The db-user system property should contain the database username. */
    public static final String DB_USER = "db-user";
    /** The db-password system property should contain the database password. */
    public static final String DB_PASSWORD = "db-password";
    
    public void loadPhenotypeAssociationsIntoSolr() throws SQLException {
        final Connection connection = this.getConnection();
    }
    
    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:postgresql://" + System.getProperty(DB_HOST) + "/" + System.getProperty(DB_NAME), System.getProperty(DB_USER), System.getProperty(DB_PASSWORD));
      }


    /**
     * For testing only.
     * @param args
     * @throws SQLException
     * @throws IOException
     */
    public static void main(String[] args) throws SQLException, IOException {
        Logger.getRootLogger().setLevel(Level.ALL);
        final Properties properties = new Properties();
        properties.load(SolrPhenotypeLoader.class.getResourceAsStream("connection.properties"));
        System.setProperties(properties);
        final SolrPhenotypeLoader loader = new SolrPhenotypeLoader();
        loader.loadPhenotypeAssociationsIntoSolr();
    }

}
