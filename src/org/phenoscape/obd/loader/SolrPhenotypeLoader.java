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

public class SolrPhenotypeLoader {

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
    private static final String PHENOTYPES_QUERY = "SELECT * FROM phenotype";
    private static final String TAXA_QUERY = "SELECT DISTINCT taxon.node_id AS taxon_node_id, taxon.uid AS taxon_uid, EXISTS (SELECT 1 FROM asserted_taxon_annotation WHERE asserted_taxon_annotation.annotation_id = taxon_annotation.annotation_id) AS some_is_asserted FROM taxon_annotation JOIN link taxon_is_a ON (taxon_is_a.predicate_id = (SELECT node.node_id FROM node WHERE node.uid = 'OBO_REL:is_a') AND taxon_is_a.node_id = taxon_annotation.taxon_node_id) JOIN node taxon ON (taxon.node_id = taxon_is_a.object_id) WHERE taxon_annotation.phenotype_node_id = ?";
    private static final String ENTITIES_QUERY = "SELECT DISTINCT entity.node_id AS entity_node_id, entity.uid AS entity_uid, phenotype.node_id AS phenotype_node_id, EXISTS (SELECT 1 FROM link WHERE link.predicate_id = (SELECT node.node_id FROM node where node.uid = 'OBO_REL:inheres_in') AND link.node_id = phenotype.node_id AND link.object_id = phenotype_inheres_in_part_of.object_id) AS strict_inheres_in FROM phenotype JOIN link phenotype_inheres_in_part_of ON (phenotype_inheres_in_part_of.predicate_id = (SELECT node.node_id FROM node where node.uid = 'OBO_REL:inheres_in_part_of') AND phenotype_inheres_in_part_of.node_id = phenotype.node_id) JOIN node entity ON (entity.node_id = phenotype_inheres_in_part_of.object_id) WHERE phenotype.node_id = ?";
    private static final String QUALITIES_QUERY = "SELECT DISTINCT quality.node_id AS quality_node_id, quality.uid AS quality_uid, phenotype.node_id AS phenotype_node_id FROM phenotype JOIN link phenotype_is_a ON (phenotype_is_a.predicate_id = (SELECT node.node_id FROM node where node.uid = 'OBO_REL:is_a') AND phenotype_is_a.node_id = phenotype.node_id) JOIN node quality ON (quality.node_id = phenotype_is_a.object_id) WHERE phenotype.node_id = ?";
    private static final String RELATED_ENTITIES_QUERY = "SELECT DISTINCT related_entity.node_id AS related_entity_node_id, related_entity.uid AS related_entity_uid, phenotype.node_id AS phenotype_node_id FROM phenotype JOIN link phenotype_towards ON (phenotype_towards.predicate_id = (SELECT node.node_id FROM node where node.uid = 'OBO_REL:towards') AND phenotype_towards.node_id = phenotype.node_id) JOIN node related_entity ON (related_entity.node_id = phenotype_towards.object_id) WHERE phenotype.node_id = ?";
    private static final String GENES_QUERY = "SELECT DISTINCT gene_node_id, gene_uid FROM distinct_gene_annotation WHERE phenotype_node_id = ?";
    private static final String GO_QUERY = String.format("SELECT DISTINCT go_term.node_id AS go_term_node_id, go_term.uid AS go_term_uid FROM distinct_gene_annotation JOIN link go_link ON (go_link.node_id = distinct_gene_annotation.gene_node_id AND go_link.predicate_id IN (SELECT node_id FROM node WHERE uid IN ('%s', '%s', '%s'))) JOIN node go_term ON (go_term.node_id = go_link.object_id) WHERE phenotype_node_id = ?", Vocab.GENE_TO_BIOLOGICAL_PROCESS_REL_ID, Vocab.GENE_TO_CELLULAR_COMPONENT_REL_ID, Vocab.GENE_TO_MOLECULAR_FUNCTION_REL_ID);
    private Connection connection;
    private SolrServer solr;
    private PreparedStatement taxaQuery;
    private PreparedStatement entitiesQuery;
    private PreparedStatement qualitiesQuery;
    private PreparedStatement relatedEntitiesQuery;
    private PreparedStatement genesQuery;
    private PreparedStatement goQuery;

    public void loadPhenotypeAssociationsIntoSolr() throws SQLException, ClassNotFoundException, IOException, ParserConfigurationException, SAXException, SolrServerException {
        this.connection = this.getConnection();
        this.solr = this.getSolrServer();
        this.clearSolrIndex();
        final PreparedStatement phenotypesQuery = this.connection.prepareStatement(PHENOTYPES_QUERY);
        this.taxaQuery = this.connection.prepareStatement(TAXA_QUERY);
        this.entitiesQuery = this.connection.prepareStatement(ENTITIES_QUERY);
        this.qualitiesQuery = this.connection.prepareStatement(QUALITIES_QUERY);
        this.relatedEntitiesQuery = this.connection.prepareStatement(RELATED_ENTITIES_QUERY);
        this.genesQuery = this.connection.prepareStatement(GENES_QUERY);
        this.goQuery = this.connection.prepareStatement(GO_QUERY);
        final ResultSet phenotypesResult = phenotypesQuery.executeQuery();
        int counter = 0;
        while (phenotypesResult.next()) {
            counter++;
            final int phenotypeNodeID = phenotypesResult.getInt("node_id");
            final String phenotypeUID = phenotypesResult.getString("uid");
            log().debug("Processing phenotype " + counter + ": " + phenotypeUID);
            final SolrInputDocument doc = this.translatePhenotype(phenotypeNodeID, phenotypeUID);
            doc.addField("id", phenotypeUID);
            this.solr.add(doc);
            this.solr.commit();
        }
    }
    
    private void clearSolrIndex() throws SolrServerException, IOException {
        this.solr.deleteByQuery("*:*");
    }
    
    private SolrInputDocument translatePhenotype(int phenotypeNodeID, String phenotypeUID) throws SQLException {
        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField("type", "phenotype");
        this.addTaxaToPhenotype(phenotypeNodeID, doc);
        this.addEntitiesToPhenotype(phenotypeNodeID, doc);
        this.addQualitiesToPhenotype(phenotypeNodeID, doc);
        this.addRelatedEntitiesToPhenotype(phenotypeNodeID, doc);
        this.addGenesToPhenotype(phenotypeNodeID, doc);
        return doc;
    }
    
    private void addTaxaToPhenotype(int phenotypeNodeID, SolrInputDocument doc) throws SQLException {
        this.taxaQuery.setInt(1, phenotypeNodeID);
        final ResultSet result = this.taxaQuery.executeQuery();
        while (result.next()) {
            final String taxonUID = result.getString("taxon_uid");
            final boolean someIsAsserted = result.getBoolean("some_is_asserted");
            doc.addField("taxon", taxonUID);
            if (someIsAsserted) {
                doc.addField("taxon_asserted", taxonUID);
            }
        }
    }
    
    private void addEntitiesToPhenotype(int phenotypeNodeID, SolrInputDocument doc) throws SQLException {
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
    
    private void addQualitiesToPhenotype(int phenotypeNodeID, SolrInputDocument doc) throws SQLException {
        this.qualitiesQuery.setInt(1, phenotypeNodeID);
        final ResultSet result = this.qualitiesQuery.executeQuery();
        while (result.next()) {
            final String qualityUID = result.getString("quality_uid");
            doc.addField("quality", qualityUID);
        }
    }
    
    private void addRelatedEntitiesToPhenotype(int phenotypeNodeID, SolrInputDocument doc) throws SQLException {
        this.relatedEntitiesQuery.setInt(1, phenotypeNodeID);
        final ResultSet result = this.relatedEntitiesQuery.executeQuery();
        while (result.next()) {
            final String relatedEntityUID = result.getString("related_entity_uid");
            doc.addField("related_entity", relatedEntityUID);
        }
    }
    
    private void addGenesToPhenotype(int phenotypeNodeID, SolrInputDocument doc) throws SQLException {
        this.genesQuery.setInt(1, phenotypeNodeID);
        final ResultSet genesResult = this.genesQuery.executeQuery();
        while (genesResult.next()) {
            final String geneObjectUID = genesResult.getString("gene_uid");
            doc.addField("gene", geneObjectUID);
        }
        this.goQuery.setInt(1, phenotypeNodeID);
        final ResultSet goResult = this.goQuery.executeQuery();
        while (goResult.next()) {
            final String goTermUID = goResult.getString("go_term_uid");
            doc.addField("gene", goTermUID);
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
        final SolrPhenotypeLoader loader = new SolrPhenotypeLoader();
        loader.loadPhenotypeAssociationsIntoSolr();
    }

}
