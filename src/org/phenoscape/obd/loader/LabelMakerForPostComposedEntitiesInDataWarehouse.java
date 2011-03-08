package org.phenoscape.obd.loader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.obd.model.CompositionalDescription;
import org.obd.model.Node;
import org.obd.query.Shard;
import org.obd.query.impl.AbstractSQLShard;
import org.obd.query.impl.OBDSQLShard;
import org.obo.app.util.Collections;



/**
 * @PURPOSE The purpose of this class is to create labels for Compositonal Descriptions
 * in a Data Warehouse table, which stores all the <TAXON><PHENOTYPE> assertions with the
 * attendant <ENTITY>, <QUALITY>, <CHARACTER> and <REIFLINKID> values. By default Compositional
 * Descriptions do not come with labels. 
 * @PROCEDURE This method uses the "simpleLabel(String)" method developed by Jim Balhoff  
 * @author cartik
 * @NOTES This class has been named as a tribute to the Seinfeld episode ' The Labelmaker'
 */

public class LabelMakerForPostComposedEntitiesInDataWarehouse {
    /** The db-host system property should contain the name of the database server. */
    public static final String DB_HOST = "db-host";
    /** The db-name system property should contain the name of the database. */
    public static final String DB_NAME = "db-name";
    /** The db-user system property should contain the database username. */
    public static final String DB_USER = "db-user";
    /** The db-password system property should contain the database password. */
    public static final String DB_PASSWORD = "db-password";
    /** The ontology-dir system property should contain the path to a folder with ontologies to be loaded. */

    /**
     * Mapping for how to represent relations used in post-comp differentia when generating a human-readable label.
     * If the relation is not in this map, use "of".
     */
    private static final Map<String, String> POSTCOMP_RELATIONS = new HashMap<String, String>();
    static {
        POSTCOMP_RELATIONS.put("OBO_REL:connected_to", "on");
        POSTCOMP_RELATIONS.put("connected_to", "on");
        POSTCOMP_RELATIONS.put("anterior_to", "anterior to");
        POSTCOMP_RELATIONS.put("BSPO:0000096", "anterior to");
        POSTCOMP_RELATIONS.put("posterior_to", "posterior to");
        POSTCOMP_RELATIONS.put("BSPO:0000099", "posterior to");
        POSTCOMP_RELATIONS.put("adjacent_to", "adjacent to");
        POSTCOMP_RELATIONS.put("OBO_REL:adjacent_to", "adjacent to");
    }

    public final String sqlQueryForPostComposedEntities = 
        "SELECT DISTINCT " +
        "entity_uid " +
        "FROM " +
        "dw_entity_table " +
        "WHERE " +
        "entity_label IS NULL";

    public final String sqlUpdateToFillInGeneratedLabelsForPostComposedEntities = 
        "UPDATE dw_entity_table " +
        "SET entity_label = ? " +
        "WHERE entity_uid = ?";

    private AbstractSQLShard shard;
    private Connection conn; 

    /**
     * Constructor initializes the shard
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public LabelMakerForPostComposedEntitiesInDataWarehouse() throws SQLException, ClassNotFoundException{
        super();
        this.shard = (AbstractSQLShard)this.initializeShard();
        this.conn = shard.getConnection();
    }

    /**
     * This method connects the shard to the database given the systems
     * parameters for DB location, name, DB username and DB password
     * @return
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    private Shard initializeShard() throws SQLException, ClassNotFoundException {
        OBDSQLShard obdsql = new OBDSQLShard();
        obdsql.connect("jdbc:postgresql://" + System.getProperty(DB_HOST) + "/" + System.getProperty(DB_NAME), System.getProperty(DB_USER), System.getProperty(DB_PASSWORD));
        return obdsql;
    }

    private Logger log() {
        return Logger.getLogger(this.getClass());
    }

    /**
     * @author jim balhoff
     * @param node
     * @return
     */
    public String simpleLabel(Node node) {
        if (node.getLabel() != null) {
            return node.getLabel();
        } else {
            final CompositionalDescription desc = this.shard.getCompositionalDescription(node.getId(), false);
            final List<String> differentia = new ArrayList<String>();
            final Collection<CompositionalDescription> differentiaArguments = desc.getDifferentiaArguments();
            if (differentiaArguments == null) {
                log().error("No differentia arguments for: " + node.getId());
                return node.getId();
            }
            for (CompositionalDescription differentium : differentiaArguments) {
                final String relationID = differentium.getRestriction().getRelationId();
                final String relationSubstitute;
                if (POSTCOMP_RELATIONS.containsKey(relationID)) {
                    relationSubstitute = POSTCOMP_RELATIONS.get(relationID);
                } else {
                    relationSubstitute = "of";
                }
                final StringBuffer buffer = new StringBuffer();
                buffer.append(" ");
                buffer.append(relationSubstitute);
                buffer.append(" ");
                buffer.append(simpleLabel(differentium.getRestriction().getTargetId()));
                differentia.add(buffer.toString());
            }
            return simpleLabel(desc.getGenus().getNodeId()) + Collections.join(differentia, ", ");
        }
    }

    public String simpleLabel(String id) {
        final Node node = this.shard.getNode(id);
        return simpleLabel(node);
    }

    /**
     * @PURPOSE This method finds all the rows with NULL values
     * for entity and updates these with a String value, which is
     * generated from the Compositional Description
     * @throws SQLException
     */
    public void makeLabel() throws SQLException{
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sqlQueryForPostComposedEntities);

        PreparedStatement ps = conn.prepareStatement(sqlUpdateToFillInGeneratedLabelsForPostComposedEntities);

        String id, label; 
        while(rs.next()){
            id = rs.getString(1);
            label = simpleLabel(id);
            ps.setString(1, label);
            ps.setString(2, id);
            ps.execute();
        }
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException{
        LabelMakerForPostComposedEntitiesInDataWarehouse lm = new LabelMakerForPostComposedEntitiesInDataWarehouse();
        lm.makeLabel();
    }

}

