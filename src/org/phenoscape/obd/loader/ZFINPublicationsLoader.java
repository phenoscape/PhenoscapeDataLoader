package org.phenoscape.obd.loader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.SQLException;

import org.obd.model.Graph;
import org.obd.model.Node;
import org.obd.query.impl.OBDSQLShard;

public class ZFINPublicationsLoader {

    /** The db-host system property should contain the name of the database server. */
    public static final String DB_HOST = "db-host";
    /** The db-name system property should contain the name of the database. */
    public static final String DB_NAME = "db-name";
    /** The db-user system property should contain the database username. */
    public static final String DB_USER = "db-user";
    /** The db-password system property should contain the database password. */
    public static final String DB_PASSWORD = "db-password";
    /** The publications-url system property should contain the URL of the ZFIN phenotypes file. */
    public static final String PUBLICATIONS_URL = "publications-url";

    public void loadPublicationsData() throws IOException, SQLException, ClassNotFoundException {
        final OBDSQLShard shard = this.initializeShard();
        final Graph graph = new Graph();
        final BufferedReader reader = this.getPublicationsData();
        String line;
        while ((line = reader.readLine()) != null) {
            graph.addNode(this.parsePublicationLine(line));
        }
        shard.putGraph(graph);
        shard.disconnect();
    }

    private OBDSQLShard initializeShard() throws SQLException, ClassNotFoundException {
        OBDSQLShard shard = new OBDSQLShard();
        shard.connect("jdbc:postgresql://" + System.getProperty(DB_HOST) + "/" + System.getProperty(DB_NAME), System.getProperty(DB_USER), System.getProperty(DB_PASSWORD));
        return shard;
    }

    private BufferedReader getPublicationsData() throws IOException {
        final URL phenotypeURL = new URL(System.getProperty(PUBLICATIONS_URL));
        return new BufferedReader(new InputStreamReader(phenotypeURL.openStream()));
    }

    private Node parsePublicationLine(String line) {
        final String[] items = line.split("\\t", -1);
        final String pubID = "ZFIN:" + items[0].trim();
        final String authors = items[2].trim();
        final String title = items[3].trim();
        final String journal = items[4].trim(); 
        final String year = items[5].trim();
        final String volume = items[6].trim(); 
        final String pages = items[7].trim();
        final Node pubNode = OBDUtil.createInstanceNode(pubID, Vocab.PUBLICATION_TYPE_ID);
        pubNode.setId(pubID);
        pubNode.setLabel(this.createFullCitation(authors, year, title, journal, volume, pages));
        return pubNode;
    }


    private String createFullCitation(String authors, String year, String title, String journal, String volume, String pages) {
        return String.format("%s. %s. %s. %s %s:%s.", authors, year, title, journal, volume, pages);
    }

    /**
     * @throws ClassNotFoundException 
     * @throws SQLException 
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
        final ZFINPublicationsLoader loader = new ZFINPublicationsLoader();
        loader.loadPublicationsData();
    }

}
