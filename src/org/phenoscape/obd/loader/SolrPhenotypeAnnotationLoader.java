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

    private static final String ANNOTATIONS_QUERY = "SELECT taxon_annotation.*, phenotype.uid AS phenotype_uid, publication.uid AS publication_uid, EXISTS (SELECT 1 FROM asserted_taxon_annotation WHERE asserted_taxon_annotation.annotation_id = taxon_annotation.annotation_id) AS is_asserted FROM taxon_annotation JOIN phenotype ON (phenotype.node_id = taxon_annotation.phenotype_node_id) LEFT JOIN annotation_source ON (annotation_source.annotation_id = taxon_annotation.annotation_id) LEFT JOIN node publication ON (publication.node_id = annotation_source.publication_node_id)";
    private static final String PHENOTYPE_ANNOTATIONS_QUERY = "SELECT *, EXISTS (SELECT 1 FROM asserted_taxon_annotation WHERE asserted_taxon_annotation.annotation_id = taxon_annotation.annotation_id) AS is_asserted FROM taxon_annotation";
    private static final String TAXA_QUERY = "SELECT DISTINCT taxon.node_id AS taxon_node_id, taxon.uid AS taxon_uid FROM taxon_annotation JOIN link taxon_is_a ON (taxon_is_a.predicate_id = (SELECT node.node_id FROM node WHERE node.uid = 'OBO_REL:is_a') AND taxon_is_a.node_id = taxon_annotation.taxon_node_id) JOIN node taxon ON (taxon.node_id = taxon_is_a.object_id) WHERE taxon_annotation.annotation_id = ?";    
    private static final String ENTITIES_QUERY = "SELECT DISTINCT entity.node_id AS entity_node_id, entity.uid AS entity_uid, EXISTS (SELECT 1 FROM link WHERE link.predicate_id = (SELECT node.node_id FROM node where node.uid = 'OBO_REL:inheres_in') AND link.node_id = phenotype.node_id AND link.object_id = phenotype_inheres_in_part_of.object_id) AS strict_inheres_in FROM taxon_annotation JOIN phenotype ON (phenotype.node_id = taxon_annotation.phenotype_node_id) JOIN link phenotype_inheres_in_part_of ON (phenotype_inheres_in_part_of.predicate_id = (SELECT node.node_id FROM node where node.uid = 'OBO_REL:inheres_in_part_of') AND phenotype_inheres_in_part_of.node_id = phenotype.node_id) JOIN node entity ON (entity.node_id = phenotype_inheres_in_part_of.object_id) WHERE taxon_annotation.annotation_id = ?";
    private static final String QUALITIES_QUERY = "SELECT DISTINCT quality.node_id AS quality_node_id, quality.uid AS quality_uid FROM taxon_annotation JOIN phenotype ON (phenotype.node_id = taxon_annotation.phenotype_node_id) JOIN link phenotype_is_a ON (phenotype_is_a.predicate_id = (SELECT node.node_id FROM node where node.uid = 'OBO_REL:is_a') AND phenotype_is_a.node_id = phenotype.node_id) JOIN node quality ON (quality.node_id = phenotype_is_a.object_id) WHERE taxon_annotation.annotation_id = ?";    
    private static final String RELATED_ENTITIES_QUERY = "SELECT DISTINCT related_entity.node_id AS related_entity_node_id, related_entity.uid AS related_entity_uid FROM taxon_annotation JOIN phenotype ON (phenotype.node_id = taxon_annotation.phenotype_node_id) JOIN link phenotype_towards ON (phenotype_towards.predicate_id = (SELECT node.node_id FROM node where node.uid = 'OBO_REL:towards') AND phenotype_towards.node_id = phenotype.node_id) JOIN node related_entity ON (related_entity.node_id = phenotype_towards.object_id) WHERE taxon_annotation.annotation_id = ?";
    private static final String PUBLICATION_QUERY = "SELECT DISTINCT publication.node_id AS publication_node_id, publication.uid AS publication_uid FROM annotation_source JOIN node publication ON (publication.node_id = annotation_source.publication_node_id) WHERE annotation_source.annotation_id = ?";

    private Connection connection;
    private SolrServer solr;
    private PreparedStatement taxaQuery;
    private PreparedStatement entitiesQuery;
    private PreparedStatement qualitiesQuery;
    private PreparedStatement relatedEntitiesQuery;
    private PreparedStatement publicationQuery;

    public void loadPhenotypeAnnotationsIntoSolr() throws SQLException, ClassNotFoundException, IOException, ParserConfigurationException, SAXException, SolrServerException {
        this.connection = this.getConnection();
        this.solr = this.getSolrServer();
        this.clearSolrIndex();
        //final PreparedStatement annotationsQuery = this.connection.prepareStatement(PHENOTYPE_ANNOTATIONS_QUERY);
        final PreparedStatement annotationsQuery = this.connection.prepareStatement(ANNOTATIONS_QUERY);
        this.taxaQuery = this.connection.prepareStatement(TAXA_QUERY);
        this.entitiesQuery = this.connection.prepareStatement(ENTITIES_QUERY);
        this.qualitiesQuery = this.connection.prepareStatement(QUALITIES_QUERY);
        this.relatedEntitiesQuery = this.connection.prepareStatement(RELATED_ENTITIES_QUERY);
        this.publicationQuery = this.connection.prepareStatement(PUBLICATION_QUERY);
        final ResultSet annotationsResult = annotationsQuery.executeQuery();
        int counter = 0;
        while (annotationsResult.next()) {
            counter++;
            //final int annotationID = annotationsResult.getInt("annotation_id");
            //final boolean asserted = annotationsResult.getBoolean("is_asserted");
            log().debug("Processing annotation " + counter);
            //final SolrInputDocument doc = this.translateAnnotation(annotationID, asserted);
            final SolrInputDocument doc = this.translateAnnotation(annotationsResult);
            this.solr.add(doc);
            this.solr.commit();
        }
    }

    private void clearSolrIndex() throws SolrServerException, IOException {
        //FIXME this won't be good when running multiple Solr loaders
        this.solr.deleteByQuery("*:*");
    }
    
    private SolrInputDocument translateAnnotation(ResultSet annotationsResult) throws SQLException {
        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField("type", "taxon_phenotype_annotation");
        doc.addField("id", "taxon_annotation_" + annotationsResult.getInt("annotation_id"));
        doc.addField("asserted", annotationsResult.getBoolean("is_asserted"));
        doc.addField("phenotype", annotationsResult.getString("phenotype_uid"));
        doc.addField("publication", annotationsResult.getString("publication_uid"));
        return doc;

    }

    private SolrInputDocument translateAnnotation(int annotationID, boolean asserted) throws SQLException {
        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField("type", "taxon_phenotype_annotation");
        doc.addField("id", "taxon_annotation_" + annotationID);
        doc.addField("asserted", asserted);
        this.addTaxaToAnnotation(annotationID, doc);
        this.addEntitiesToAnnotation(annotationID, doc);
        this.addQualitiesToAnnotation(annotationID, doc);
        this.addRelatedEntitiesToAnnotation(annotationID, doc);
        this.addPublicationToAnnotation(annotationID, doc);
        return doc;
    }

    private void addTaxaToAnnotation(int annotationID, SolrInputDocument doc) throws SQLException {
        this.taxaQuery.setInt(1, annotationID);
        final ResultSet result = this.taxaQuery.executeQuery();
        while (result.next()) {
            final String taxonUID = result.getString("taxon_uid");
            doc.addField("taxon", taxonUID);
        }
    }

    private void addEntitiesToAnnotation(int annotationID, SolrInputDocument doc) throws SQLException {
        this.entitiesQuery.setInt(1, annotationID);
        final ResultSet result = this.entitiesQuery.executeQuery();
        while (result.next()) {
            final String entityUID = result.getString("entity_uid");
            final boolean strictInheresIn = result.getBoolean("strict_inheres_in");
            doc.addField("entity", entityUID);
            if (strictInheresIn) {
                doc.addField("entity_strict_inheres_in", entityUID);
            }
        }
    }

    private void addQualitiesToAnnotation(int annotationID, SolrInputDocument doc) throws SQLException {
        this.qualitiesQuery.setInt(1, annotationID);
        final ResultSet result = this.qualitiesQuery.executeQuery();
        while (result.next()) {
            final String qualityUID = result.getString("quality_uid");
            doc.addField("quality", qualityUID);
        }
    }

    private void addRelatedEntitiesToAnnotation(int annotationID, SolrInputDocument doc) throws SQLException {
        this.relatedEntitiesQuery.setInt(1, annotationID);
        final ResultSet result = this.relatedEntitiesQuery.executeQuery();
        while (result.next()) {
            final String relatedEntityUID = result.getString("related_entity_uid");
            doc.addField("related_entity", relatedEntityUID);
        }
    }

    private void addPublicationToAnnotation(int annotationID, SolrInputDocument doc) throws SQLException {
        this.publicationQuery.setInt(1, annotationID);
        final ResultSet result = this.publicationQuery.executeQuery();
        while (result.next()) {
            final String publicationUID = result.getString("publication_uid");
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
//        Logger.getRootLogger().setLevel(Level.ALL);
//        final Properties properties = new Properties();
//        properties.load(SolrPhenotypeLoader.class.getResourceAsStream("connection.properties"));
//        for (Entry<Object, Object> entry : properties.entrySet()) {
//            System.setProperty(entry.getKey().toString(), entry.getValue().toString());
//        }

        final SolrPhenotypeAnnotationLoader loader = new SolrPhenotypeAnnotationLoader();
        loader.loadPhenotypeAnnotationsIntoSolr();
    }

}
