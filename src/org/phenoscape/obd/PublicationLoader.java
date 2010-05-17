package org.phenoscape.obd;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.obd.model.Graph;
import org.obd.query.impl.OBDSQLShard;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class PublicationLoader {

	/** The db-host system property should contain the name of the database server. */
    public static final String DB_HOST = "db-host";
    /** The db-name system property should contain the name of the database. */
    public static final String DB_NAME = "db-name";
    /** The db-user system property should contain the database username. */
    public static final String DB_USER = "db-user";
    /** The db-password system property should contain the database password. */
    public static final String DB_PASSWORD = "db-password";
    /** The ontology-dir system property should contain the path to a folder with ontologies to be loaded. */
    public static final String PUBLICATION_DIR = "publication-dir";    
	
	public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException, SQLException, ClassNotFoundException {
	    final DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
        final DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
        final Document xmlDoc = docBuilder.parse(new File(System.getProperty(PUBLICATION_DIR)));
        final OBDPublicationBridge bridge = new OBDPublicationBridge();
        final Graph graph = bridge.translate(xmlDoc);
        final OBDSQLShard shard = new OBDSQLShard();
        shard.connect("jdbc:postgresql://" + System.getProperty(DB_HOST) + "/" + System.getProperty(DB_NAME), System.getProperty(DB_USER), System.getProperty(DB_PASSWORD));
        shard.putGraph(graph);
        shard.disconnect();
	}

}
