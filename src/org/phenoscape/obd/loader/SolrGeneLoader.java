package org.phenoscape.obd.loader;

import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.xml.sax.SAXException;

public class SolrGeneLoader {

    /** The db-host system property should contain the name of the database server. */
    public static final String DB_HOST = "db-host";
    /** The db-name system property should contain the name of the database. */
    public static final String DB_NAME = "db-name";
    /** The db-user system property should contain the database username. */
    public static final String DB_USER = "db-user";
    /** The db-password system property should contain the database password. */
    public static final String DB_PASSWORD = "db-password";
    /** The solr-url system property should contain the url for the Solr web application. */
    public static final String SOLR_URL = "solr-url";
    private static final String GENES = "SELECT DISTINCT gene.* FROM gene JOIN distinct_gene_annotation ON (distinct_gene_annotation.gene_node_id = gene.node_id)";
    private static final String PHENOTYPES = "SELECT DISTINCT phenotype_uid FROM distinct_gene_annotation WHERE gene_node_id = ?";
    private Connection connection;
    private SolrServer solr;
    private PreparedStatement phenotypesQuery;

    public void loadGenesIntoSolr() throws SQLException, ClassNotFoundException, IOException, ParserConfigurationException, SAXException, SolrServerException {
        this.connection = this.getConnection();
        this.solr = this.getSolrServer();
        final PreparedStatement genesQuery = this.connection.prepareStatement(GENES);
        this.phenotypesQuery = this.connection.prepareStatement(PHENOTYPES);
        final ResultSet genesResult = genesQuery.executeQuery();
        int counter = 0;
        while (genesResult.next()) {
            counter++;
            final String geneUID = genesResult.getString("uid");
            log().debug("Processing annotation " + counter);
            final SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", geneUID);
            doc.addField("type", "gene");
            doc.addField("label", genesResult.getString("label"));
            doc.addField("full_name", genesResult.getString("full_name"));
            this.addPhenotypesToGene(genesResult.getInt("node_id"), doc);
            this.solr.add(doc);
        }
        this.solr.commit();
    }

    private void addPhenotypesToGene(int geneNodeID, SolrInputDocument doc) throws SQLException {
        this.phenotypesQuery.setInt(1, geneNodeID);
        final ResultSet result = this.phenotypesQuery.executeQuery();
        while (result.next()) {
            final String phenotypeUID = result.getString("uid");
            doc.addField("phenotype", phenotypeUID);
        }
    }

    private Connection getConnection() throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        return DriverManager.getConnection("jdbc:postgresql://" + System.getProperty(DB_HOST) + "/" + System.getProperty(DB_NAME), System.getProperty(DB_USER), System.getProperty(DB_PASSWORD));
    }

    private SolrServer getSolrServer() throws MalformedURLException {
        return new CommonsHttpSolrServer(System.getProperty(SOLR_URL));
    }

    private Logger log() {
        return Logger.getLogger(this.getClass());
    }


    /**
     * @throws SQLException
     * @throws IOException
     * @throws ClassNotFoundException 
     * @throws SAXException 
     * @throws ParserConfigurationException 
     * @throws SolrServerException 
     */
    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException, ParserConfigurationException, SAXException, SolrServerException {
        //        Logger.getRootLogger().setLevel(Level.ALL);
        //        final Properties properties = new Properties();
        //        properties.load(SolrGeneAnnotationLoader.class.getResourceAsStream("connection.properties"));
        //        for (Entry<Object, Object> entry : properties.entrySet()) {
        //            System.setProperty(entry.getKey().toString(), entry.getValue().toString());
        //        }

        final SolrGeneLoader loader = new SolrGeneLoader();
        loader.loadGenesIntoSolr();
    }

}
