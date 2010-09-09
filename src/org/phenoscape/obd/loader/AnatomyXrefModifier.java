package org.phenoscape.obd.loader;

import java.sql.SQLException;
import java.util.Collection;

import org.apache.commons.lang.ObjectUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.obd.model.Graph;
import org.obd.model.LinkStatement;
import org.obd.model.Node;
import org.obd.model.Statement;
import org.obd.query.Shard;
import org.obd.query.impl.OBDSQLShard;

public class AnatomyXrefModifier {

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

    public AnatomyXrefModifier() throws SQLException, ClassNotFoundException {
        this.shard = this.initializeShard();
    }

    public void modifyXrefs() {
        log().debug("Starting");
        final Graph graph = new Graph();
        final Collection<Node> zfaTerms = this.shard.getNodesBySource("zebrafish_anatomy");
        for (Node term : zfaTerms) {
            log().debug("Term: " + term);
            //final Collection<Statement> xrefLinks = this.shard.getStatements(term.getId(), Vocab.HAS_DBXREF, null, null, false, false);
            //final Collection<Statement> xrefLinks = Arrays.asList(term.getStatements());
            final Collection<Statement> xrefLinks = this.shard.getStatementsByNode(term.getId());
            String anatomyXrefID = null;
            String taoID = null;
            for (Statement link : xrefLinks) {
                if (link.getRelationId().equals(Vocab.HAS_DBXREF)) {
                    final String targetID = link.getTargetId();
                    final String targetSource = this.shard.getNode(targetID).getSourceId();
                    if (ObjectUtils.equals(targetSource, "teleost_anatomy")) {
                        log().debug("Found TAO term: " + targetID);
                        taoID = targetID;
                    } else if (targetID.startsWith("ZFIN:ZDB-ANAT")) {
                        log().debug("Found anatomy xref: " + targetID);
                        anatomyXrefID = targetID;
                    }
                }
            }
            if ((anatomyXrefID != null) && (taoID != null)) {
                log().debug("Linking: " + taoID + ", " + anatomyXrefID);
                graph.addStatement(new LinkStatement(taoID, Vocab.HAS_DBXREF, anatomyXrefID));
            }
        }
        this.shard.putGraph(graph);
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Logger.getRootLogger().setLevel(Level.ALL);
        final AnatomyXrefModifier xrefModifier = new AnatomyXrefModifier();
        xrefModifier.modifyXrefs();
    }

    private Shard initializeShard() throws SQLException, ClassNotFoundException {
        final OBDSQLShard obdsql = new OBDSQLShard();
        obdsql.connect("jdbc:postgresql://" + System.getProperty(DB_HOST) + "/" + System.getProperty(DB_NAME), System.getProperty(DB_USER), System.getProperty(DB_PASSWORD));
        return obdsql;
    }

    @SuppressWarnings("unused")
    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
