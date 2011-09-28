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

public class SolrPhenotypeAnnotationLoader {

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

    private static final String ANNOTATIONS_QUERY = "SELECT taxon_annotation.*, phenotype.uid AS phenotype_uid, phenotype.entity_uid, phenotype.entity_label, phenotype.quality_uid, phenotype.quality_label, phenotype.related_entity_uid, phenotype.related_entity_label, taxon.uid AS taxon_uid, taxon.label AS taxon_label, taxon.rank_uid, taxon.is_extinct, EXISTS (SELECT 1 FROM asserted_taxon_annotation WHERE asserted_taxon_annotation.annotation_id = taxon_annotation.annotation_id) AS is_asserted FROM taxon_annotation JOIN phenotype ON (phenotype.node_id = taxon_annotation.phenotype_node_id) JOIN taxon ON (taxon.node_id = taxon_annotation.taxon_node_id) ORDER BY annotation_id LIMIT 10000 OFFSET ?";

    private Connection connection;
    private SolrServer solr;

    public void loadPhenotypeAnnotationsIntoSolr() throws SQLException, ClassNotFoundException, IOException, ParserConfigurationException, SAXException, SolrServerException {
        this.connection = this.getConnection();
        this.solr = this.getSolrServer();
        final PreparedStatement annotationsQuery = this.connection.prepareStatement(ANNOTATIONS_QUERY);
        int offset = 0;
        boolean more = true;
        int counter = 0;
        while (more) {
            int iterationCounter = 0;
            offset += 10000;
            annotationsQuery.setInt(1, offset);
            final ResultSet annotationsResult = annotationsQuery.executeQuery();
            while (annotationsResult.next()) {
                iterationCounter++;
                counter++;
                log().debug("Processing annotation " + counter);
                final SolrInputDocument doc = this.translateAnnotation(annotationsResult);
                this.solr.add(doc);
            }
            this.solr.commit();
            if (iterationCounter < 10000) { more = false; }
        }
    }

    private SolrInputDocument translateAnnotation(ResultSet annotationsResult) throws SQLException {
        final SolrInputDocument doc = new SolrInputDocument();
        final String taxonUID = annotationsResult.getString("taxon_uid");
        final String phenotypeUID = annotationsResult.getString("phenotype_uid");
        doc.addField("type", "taxon_phenotype_annotation");
        doc.addField("id", getAnnotationID(taxonUID, phenotypeUID));
        doc.addField("asserted", annotationsResult.getBoolean("is_asserted"));
        doc.addField("phenotype", annotationsResult.getString("phenotype_uid"));
        doc.addField("direct_taxon", annotationsResult.getString("taxon_uid"));
        doc.addField("direct_taxon_label", annotationsResult.getString("taxon_label"));
        doc.addField("is_extinct", annotationsResult.getBoolean("is_extinct"));
        doc.addField("direct_entity", annotationsResult.getString("entity_uid"));
        doc.addField("direct_entity_label", annotationsResult.getString("entity_label"));
        doc.addField("direct_quality", annotationsResult.getString("quality_uid"));
        doc.addField("direct_quality_label", annotationsResult.getString("quality_label"));
        doc.addField("direct_related_entity", annotationsResult.getString("related_entity_uid"));
        doc.addField("direct_related_entity_label", annotationsResult.getString("related_entity_label"));
        return doc;
    }
    
    public static String getAnnotationID(String taxonUID, String phenotypeUID) {
        return "taxon_annotation_" + taxonUID + "#" + phenotypeUID;
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
//        properties.load(SolrPhenotypeAnnotationLoader.class.getResourceAsStream("connection.properties"));
//        for (Entry<Object, Object> entry : properties.entrySet()) {
//            System.setProperty(entry.getKey().toString(), entry.getValue().toString());
//        }

        final SolrPhenotypeAnnotationLoader loader = new SolrPhenotypeAnnotationLoader();
        loader.loadPhenotypeAnnotationsIntoSolr();
    }

}
