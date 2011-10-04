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

public class SolrGeneAnnotationLoader {

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
    private static final String GENE_ANNOTATIONS_QUERY = "SELECT distinct_gene_annotation.*, entity_label.simple_label AS direct_entity_simple_label, quality_label.simple_label AS direct_quality_simple_label, related_entity_label.simple_label AS direct_related_entity_simple_label FROM distinct_gene_annotation JOIN smart_node_label entity_label ON (entity_label.node_id = distinct_gene_annotation.entity_node_id) JOIN smart_node_label quality_label ON (quality_label.node_id = distinct_gene_annotation.quality_node_id) JOIN smart_node_label related_entity_label ON (related_entity_label.node_id = distinct_gene_annotation.related_entity_node_id)";
    private static final String ENTITIES_QUERY = "SELECT DISTINCT entity.node_id AS entity_node_id, entity.uid AS entity_uid, phenotype.node_id AS phenotype_node_id, EXISTS (SELECT 1 FROM link WHERE link.predicate_id = (SELECT node.node_id FROM node where node.uid = 'OBO_REL:inheres_in') AND link.node_id = phenotype.node_id AND link.object_id = phenotype_inheres_in_part_of.object_id) AS strict_inheres_in FROM phenotype JOIN link phenotype_inheres_in_part_of ON (phenotype_inheres_in_part_of.predicate_id = (SELECT node.node_id FROM node where node.uid = 'OBO_REL:inheres_in_part_of') AND phenotype_inheres_in_part_of.node_id = phenotype.node_id) JOIN node entity ON (entity.node_id = phenotype_inheres_in_part_of.object_id) WHERE phenotype.node_id = ?";
    private static final String QUALITIES_QUERY = "SELECT DISTINCT quality.node_id AS quality_node_id, quality.uid AS quality_uid, phenotype.node_id AS phenotype_node_id FROM phenotype JOIN link phenotype_is_a ON (phenotype_is_a.predicate_id = (SELECT node.node_id FROM node where node.uid = 'OBO_REL:is_a') AND phenotype_is_a.node_id = phenotype.node_id) JOIN node quality ON (quality.node_id = phenotype_is_a.object_id) WHERE phenotype.node_id = ?";
    private static final String RELATED_ENTITIES_QUERY = "SELECT DISTINCT related_entity.node_id AS related_entity_node_id, related_entity.uid AS related_entity_uid, phenotype.node_id AS phenotype_node_id FROM phenotype JOIN link phenotype_towards ON (phenotype_towards.predicate_id = (SELECT node.node_id FROM node where node.uid = 'OBO_REL:towards') AND phenotype_towards.node_id = phenotype.node_id) JOIN node related_entity ON (related_entity.node_id = phenotype_towards.object_id) WHERE phenotype.node_id = ?";
    private Connection connection;
    private SolrServer solr;
    private PreparedStatement entitiesQuery;
    private PreparedStatement qualitiesQuery;
    private PreparedStatement relatedEntitiesQuery;

    public void loadGeneAnnotationsIntoSolr() throws SQLException, ClassNotFoundException, IOException, ParserConfigurationException, SAXException, SolrServerException {
        this.connection = this.getConnection();
        this.solr = this.getSolrServer();
        final PreparedStatement annotationsQuery = this.connection.prepareStatement(GENE_ANNOTATIONS_QUERY);
        this.entitiesQuery = this.connection.prepareStatement(ENTITIES_QUERY);
        this.qualitiesQuery = this.connection.prepareStatement(QUALITIES_QUERY);
        this.relatedEntitiesQuery = this.connection.prepareStatement(RELATED_ENTITIES_QUERY);
        final ResultSet annotationsResult = annotationsQuery.executeQuery();
        int counter = 0;
        while (annotationsResult.next()) {
            counter++;
            final String geneUID = annotationsResult.getString("gene_uid");
            final String phenotypeUID = annotationsResult.getString("phenotype_uid");
            log().debug("Processing annotation " + counter);
            final SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", getAnnotationID(geneUID, phenotypeUID));
            doc.addField("type", "gene_annotation");
            doc.addField("direct_gene", geneUID);
            doc.addField("direct_gene_label", annotationsResult.getString("gene_label"));
            doc.addField("direct_entity", annotationsResult.getString("entity_uid"));
            doc.addField("direct_entity_label", annotationsResult.getString("direct_entity_simple_label"));
            doc.addField("direct_quality", annotationsResult.getString("quality_uid"));
            doc.addField("direct_quality_label", annotationsResult.getString("direct_quality_simple_label"));
            doc.addField("direct_related_entity", annotationsResult.getString("related_entity_uid"));
            doc.addField("direct_related_entity_label", annotationsResult.getString("direct_related_entity_simple_label"));
            final int phenotypeNodeID = annotationsResult.getInt("phenotype_node_id");
            this.addEntitiesToAnnotation(phenotypeNodeID, doc);
            this.addQualitiesToAnnotation(phenotypeNodeID, doc);
            this.addRelatedEntitiesToAnnotation(phenotypeNodeID, doc);
            this.solr.add(doc);
        }
        this.solr.commit();
    }

    private void addEntitiesToAnnotation(int phenotypeNodeID, SolrInputDocument doc) throws SQLException {
        this.entitiesQuery.setInt(1, phenotypeNodeID);
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

    private void addQualitiesToAnnotation(int phenotypeNodeID, SolrInputDocument doc) throws SQLException {
        this.qualitiesQuery.setInt(1, phenotypeNodeID);
        final ResultSet result = this.qualitiesQuery.executeQuery();
        while (result.next()) {
            final String qualityUID = result.getString("quality_uid");
            doc.addField("quality", qualityUID);
        }
    }

    private void addRelatedEntitiesToAnnotation(int phenotypeNodeID, SolrInputDocument doc) throws SQLException {
        this.relatedEntitiesQuery.setInt(1, phenotypeNodeID);
        final ResultSet result = this.relatedEntitiesQuery.executeQuery();
        while (result.next()) {
            final String relatedEntityUID = result.getString("related_entity_uid");
            doc.addField("related_entity", relatedEntityUID);
        }
    }

    private Connection getConnection() throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        return DriverManager.getConnection("jdbc:postgresql://" + System.getProperty(DB_HOST) + "/" + System.getProperty(DB_NAME), System.getProperty(DB_USER), System.getProperty(DB_PASSWORD));
    }

    public static String getAnnotationID(String geneUID, String phenotypeUID) {
        return "gene_annotation_" + geneUID + "#" + phenotypeUID;
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

        final SolrGeneAnnotationLoader loader = new SolrGeneAnnotationLoader();
        loader.loadGeneAnnotationsIntoSolr();
    }

}
