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

public class SolrPublicationLoader {

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

    private static final String PUBLICATIONS_QUERY = "SELECT DISTINCT publication.node_id, publication.uid, publication.label FROM node publication JOIN annotation_source ON (annotation_source.publication_node_id = publication.node_id)";
    private static final String TAXA_QUERY = "SELECT DISTINCT taxon.uid FROM annotation_source JOIN taxon_annotation ON (taxon_annotation.annotation_id = annotation_source.annotation_id) JOIN link taxon_is_a ON (taxon_is_a.node_id = taxon_annotation.taxon_node_id AND taxon_is_a.predicate_id = (SELECT node_id FROM node WHERE uid = 'OBO_REL:is_a')) JOIN taxon ON (taxon.node_id = taxon_is_a.object_id) WHERE annotation_source.publication_node_id = ?";
    private static final String PHENOTYPES_QUERY = "SELECT DISTINCT phenotype.uid FROM annotation_source JOIN taxon_annotation ON (taxon_annotation.annotation_id = annotation_source.annotation_id) JOIN phenotype ON (phenotype.node_id = taxon_annotation.phenotype_node_id) WHERE annotation_source.publication_node_id = ?";
    private static final String ANNOTATIONS_QUERY = "SELECT DISTINCT taxon.uid AS taxon_uid, phenotype.uid AS phenotype_uid FROM annotation_source JOIN taxon_annotation ON (taxon_annotation.annotation_id = annotation_source.annotation_id) JOIN phenotype ON (phenotype.node_id = taxon_annotation.phenotype_node_id) JOIN taxon ON (taxon.node_id = taxon_annotation.taxon_node_id) WHERE annotation_source.publication_node_id = ?";

    private Connection connection;
    private SolrServer solr;
    private PreparedStatement taxaQuery;
    private PreparedStatement phenotypesQuery;
    private PreparedStatement annotationsQuery;

    public void loadTaxaIntoSolr() throws SQLException, ClassNotFoundException, IOException, ParserConfigurationException, SAXException, SolrServerException {
        this.connection = this.getConnection();
        this.solr = this.getSolrServer();
        this.taxaQuery = this.connection.prepareStatement(TAXA_QUERY);
        this.phenotypesQuery = this.connection.prepareStatement(PHENOTYPES_QUERY);
        this.annotationsQuery = this.connection.prepareStatement(ANNOTATIONS_QUERY);
        final ResultSet pubsResult = this.connection.prepareStatement(PUBLICATIONS_QUERY).executeQuery();
        int counter = 0;
        while (pubsResult.next()) {
            counter++;
            final int pubNodeID = pubsResult.getInt("node_id");
            final String pubUID = pubsResult.getString("uid");
            final String pubLabel = pubsResult.getString("label");
            log().debug("Processing publication " + counter + ": " + pubUID);
            final SolrInputDocument doc = new SolrInputDocument();
            doc.addField("type", "publication");
            doc.addField("id", pubUID);
            doc.addField("label", pubLabel);
            this.addTaxaToPublication(pubNodeID, doc);
            this.addPhenotypesToPublication(pubNodeID, doc);
            this.addAnnotationsToPublication(pubNodeID, doc);
            this.solr.add(doc);
        }
        this.solr.commit();
    }

    private void addTaxaToPublication(int pubNodeID, SolrInputDocument doc) throws SQLException {
        this.taxaQuery.setInt(1, pubNodeID);
        final ResultSet result = this.taxaQuery.executeQuery();
        while (result.next()) {
            final String taxonUID = result.getString("uid");
            doc.addField("taxon", taxonUID);
        }
    }

    private void addPhenotypesToPublication(int pubNodeID, SolrInputDocument doc) throws SQLException {
        this.phenotypesQuery.setInt(1, pubNodeID);
        final ResultSet result = this.phenotypesQuery.executeQuery();
        while (result.next()) {
            final String phenotypeUID = result.getString("uid");
            doc.addField("phenotype", phenotypeUID);
        }
    }

    private void addAnnotationsToPublication(int pubNodeID, SolrInputDocument doc) throws SQLException {
        this.annotationsQuery.setInt(1, pubNodeID);
        final ResultSet result = this.annotationsQuery.executeQuery();
        while (result.next()) {
            final String taxonUID = result.getString("taxon_uid");
            final String phenotypeUID = result.getString("phenotype_uid");
            final String annotationID = SolrPhenotypeAnnotationLoader.getAnnotationID(taxonUID, phenotypeUID);
            doc.addField("annotation", annotationID);
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
//        properties.load(SolrPublicationLoader.class.getResourceAsStream("connection.properties"));
//        for (Entry<Object, Object> entry : properties.entrySet()) {
//            System.setProperty(entry.getKey().toString(), entry.getValue().toString());
//        }

        final SolrPublicationLoader loader = new SolrPublicationLoader();
        loader.loadTaxaIntoSolr();
    }

}
