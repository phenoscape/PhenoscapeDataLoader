package org.phenoscape.obd.loader;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.bbop.dataadapter.DataAdapterException;
import org.obd.model.CompositionalDescription;
import org.obd.model.Graph;
import org.obd.model.CompositionalDescription.Predicate;
import org.obd.query.Shard;
import org.obd.query.impl.OBDSQLShard;
import org.obo.dataadapter.OBOAdapter;
import org.obo.dataadapter.OBOFileAdapter;
import org.obo.datamodel.IdentifiedObject;
import org.obo.datamodel.OBOClass;
import org.obo.datamodel.OBOSession;
import org.obo.datamodel.TermSubset;
import org.purl.obo.vocab.RelationVocabulary;

public class AttributePhenotypesLoader {

    /** The db-host system property should contain the name of the database server. */
    public static final String DB_HOST = "db-host";
    /** The db-name system property should contain the name of the database. */
    public static final String DB_NAME = "db-name";
    /** The db-user system property should contain the database username. */
    public static final String DB_USER = "db-user";
    /** The db-password system property should contain the database password. */
    public static final String DB_PASSWORD = "db-password";
    /** The ontology-dir system property should contain the path to a folder with ontologies to be loaded. */
    public static final String ONTOLOGY_DIR = "ontology-dir";

    private Shard shard;
    private OBOSession session;

    public AttributePhenotypesLoader() throws SQLException, ClassNotFoundException {
        this.shard = this.initializeShard();
        this.session = this.loadOBOSession();
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        final AttributePhenotypesLoader loader = new AttributePhenotypesLoader();
        loader.loadAttributePhenotypes();
    }
    
    public void loadAttributePhenotypes() {
        final Graph graph = new Graph();
        for (OBOClass attribute : this.findAttributes()) {
            for (IdentifiedObject io : this.session.getObjects()) {
                if (io instanceof OBOClass) {
                    final OBOClass term = (OBOClass)io;
                    if ((term.getNamespace() != null) && (term.getNamespace().getID().equals("teleost_anatomy"))) {
                        final CompositionalDescription phenotype = this.createPhenotype(term, attribute);
                        log().info("Adding phenotype: " + phenotype);
                        graph.addStatements(phenotype);
                    }
                }
            }
        }
        this.shard.putGraph(graph);
    }
    
    private CompositionalDescription createPhenotype(OBOClass entity, OBOClass attribute) {
        final CompositionalDescription phenotypeNode = new CompositionalDescription(Predicate.INTERSECTION);
        phenotypeNode.addArgument(OBDUtil.translateOBOClass(attribute));
        // check to avoid a NullPointerException
        if (entity.getParents() != null) {
            phenotypeNode.addArgument((new RelationVocabulary()).inheres_in(), OBDUtil.translateOBOClass(entity));
        }
        phenotypeNode.setId(phenotypeNode.generateId());
        return phenotypeNode;
    }
    
    private List<OBOClass> findAttributes() {
        final List<OBOClass> attributes = new ArrayList<OBOClass>();
        for (IdentifiedObject io : this.session.getObjects()) {
            if (io instanceof OBOClass) {
                final OBOClass potentialAttribute = (OBOClass)io;
                for (TermSubset subset : potentialAttribute.getSubsets()) {
                    if (subset.getName().equals("character_slim")) {
                        attributes.add(potentialAttribute);
                    }
                }
            }
        }
        return attributes;
    }

    private OBOSession loadOBOSession() {
        final OBOFileAdapter fileAdapter = new OBOFileAdapter();
        OBOFileAdapter.OBOAdapterConfiguration config = new OBOFileAdapter.OBOAdapterConfiguration();
        config.setReadPaths(this.getOntologyPaths());
        config.setBasicSave(false);
        config.setAllowDangling(true);
        config.setFollowImports(false);
        try {
            return fileAdapter.doOperation(OBOAdapter.READ_ONTOLOGY, config, null);
        } catch (DataAdapterException e) {
            log().fatal("Failed to load ontologies", e);
            return null;
        }
    }

    private List<String> getOntologyPaths() {
        List<String> paths = new ArrayList<String>();
        File ontCache = new File(System.getProperty(ONTOLOGY_DIR));
        for (File f : ontCache.listFiles()) {
            paths.add(f.getAbsolutePath());
        }
        return paths;
    }

    private Shard initializeShard() throws SQLException, ClassNotFoundException {
        OBDSQLShard obdsql = new OBDSQLShard();
        obdsql.connect("jdbc:postgresql://" + System.getProperty(DB_HOST) + "/" + System.getProperty(DB_NAME), System.getProperty(DB_USER), System.getProperty(DB_PASSWORD));
        return obdsql;
    }

    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
