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

public class SolrTaxonLoader {

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
    private static final String TAXA_QUERY = "SELECT * FROM taxon";
    private static final String PARENT_TAXA_QUERY = "SELECT DISTINCT parent.uid FROM taxon parent JOIN link taxon_is_a ON (taxon_is_a.node_id = ? AND taxon_is_a.object_id = parent.node_id AND taxon_is_a.predicate_id = (SELECT node_id FROM node WHERE uid = 'OBO_REL:is_a'))";
    private static final String PHENOTYPES_QUERY = "SELECT DISTINCT phenotype.uid, EXISTS (SELECT 1 FROM asserted_taxon_annotation WHERE asserted_taxon_annotation.annotation_id = taxon_annotation.annotation_id) AS asserted FROM taxon_annotation JOIN phenotype ON (phenotype.node_id = taxon_annotation.phenotype_node_id) WHERE taxon_annotation.taxon_node_id = ?";
    private static final String PUBLICATIONS_QUERY = "SELECT DISTINCT publication.uid FROM node publication JOIN annotation_source ON (annotation_source.publication_node_id = publication.node_id) JOIN taxon_annotation ON (taxon_annotation.annotation_id = annotation_source.annotation_id) WHERE taxon_annotation.taxon_node_id = ?";

    private Connection connection;
    private SolrServer solr;
    private PreparedStatement parentTaxaQuery;
    private PreparedStatement phenotypesQuery;
    private PreparedStatement publicationsQuery;

    public void loadTaxaIntoSolr() throws SQLException, ClassNotFoundException, IOException, ParserConfigurationException, SAXException, SolrServerException {
        this.connection = this.getConnection();
        this.solr = this.getSolrServer();
        this.parentTaxaQuery = this.connection.prepareStatement(PARENT_TAXA_QUERY);
        this.phenotypesQuery = this.connection.prepareStatement(PHENOTYPES_QUERY);
        this.publicationsQuery = this.connection.prepareStatement(PUBLICATIONS_QUERY);
        final ResultSet taxaResult = this.connection.prepareStatement(TAXA_QUERY).executeQuery();
        int counter = 0;
        while (taxaResult.next()) {
            counter++;
            final int taxonNodeID = taxaResult.getInt("node_id");
            final String taxonUID = taxaResult.getString("uid");
            final String taxonLabel = taxaResult.getString("label");
            log().debug("Processing taxon " + counter + ": " + taxonUID);
            final SolrInputDocument doc = new SolrInputDocument();
            doc.addField("type", "taxon");
            doc.addField("id", taxonUID);
            doc.addField("label", taxonLabel);
            final String rankUID = taxaResult.getString("rank_uid");
            if (rankUID != null) {
                doc.addField("rank", rankUID);
            }
            final boolean isExtinct = taxaResult.getBoolean("is_extinct");
            doc.addField("is_extinct", isExtinct);
            final String orderUID = taxaResult.getString("order_uid");
            if (orderUID != null) {
                doc.addField("order", orderUID);
            }
            final String orderLabel = taxaResult.getString("order_label");
            if (orderLabel != null) {
                doc.addField("order_label", orderLabel);
            }
            final boolean orderIsExtinct = taxaResult.getBoolean("order_is_extinct");
            doc.addField("order_is_extinct", orderIsExtinct);
            final String familyUID = taxaResult.getString("family_uid");
            if (familyUID != null) {
                doc.addField("family", familyUID);
            }
            final String familyLabel = taxaResult.getString("family_label");
            if (familyLabel != null) {
                doc.addField("family_label", familyLabel);
            }
            final boolean familyIsExtinct = taxaResult.getBoolean("family_is_extinct");
            doc.addField("family_is_extinct", familyIsExtinct);
            this.addParentTaxaToTaxon(taxonNodeID, doc);
            this.addPhenotypesToTaxon(taxonNodeID, doc);
            this.addPublicationsToTaxon(taxonNodeID, doc);
            this.solr.add(doc);
        }
        this.solr.commit();
    }
    
    private void addParentTaxaToTaxon(int taxonNodeID, SolrInputDocument doc) throws SQLException {
        this.parentTaxaQuery.setInt(1, taxonNodeID);
        final ResultSet result = this.parentTaxaQuery.executeQuery();
        while (result.next()) {
            final String parentUID = result.getString("uid");
            doc.addField("subtaxon_of", parentUID);
        }
    }

    private void addPhenotypesToTaxon(int taxonNodeID, SolrInputDocument doc) throws SQLException {
        this.phenotypesQuery.setInt(1, taxonNodeID);
        final ResultSet result = this.phenotypesQuery.executeQuery();
        while (result.next()) {
            final String phenotypeUID = result.getString("uid");
            final boolean asserted = result.getBoolean("asserted");
            doc.addField("phenotype", phenotypeUID);
            if (asserted) {
                doc.addField("phenotype_asserted", phenotypeUID);
            }
        }
    }

    private void addPublicationsToTaxon(int taxonNodeID, SolrInputDocument doc) throws SQLException {
        this.publicationsQuery.setInt(1, taxonNodeID);
        final ResultSet result = this.publicationsQuery.executeQuery();
        while (result.next()) {
            final String publicationUID = result.getString("uid");
            doc.addField("publication", publicationUID);
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
//                Logger.getRootLogger().setLevel(Level.ALL);
//                final Properties properties = new Properties();
//                properties.load(SolrTaxonLoader.class.getResourceAsStream("connection.properties"));
//                for (Entry<Object, Object> entry : properties.entrySet()) {
//                    System.setProperty(entry.getKey().toString(), entry.getValue().toString());
//                }

        final SolrTaxonLoader loader = new SolrTaxonLoader();
        loader.loadTaxaIntoSolr();
    }

}
