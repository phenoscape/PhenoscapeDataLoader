package org.phenoscape.obd.loader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.obd.query.Shard;
import org.obd.query.impl.AbstractSQLShard;
import org.obd.query.impl.OBDSQLShard;

public class GeneAnnotationCounter {
	/**
	 * This class counts the phenotypes associated with every gene
	 * and loads them into a static text file
	 */
	
	/** The db-host system property should contain the name of the database server. */
    public static final String DB_HOST = "db-host";
    /** The db-name system property should contain the name of the database. */
    public static final String DB_NAME = "db-name";
    /** The db-user system property should contain the database username. */
    public static final String DB_USER = "db-user";
    /** The db-password system property should contain the database password. */
    public static final String DB_PASSWORD = "db-password";
    /** The file-loc system property should contain the location where this file will be stored */
    public static final String TEXT_FILE_LOC = "file-loc";

    private Shard shard;
    private Connection conn;

    /**
	 * @INPUT - None
     * This query retrieves the number of annotations for every GENE. 
     * These counts are returned in order from 
     * the highest to the lowest
     */
    private String geneCountQuery = 
    	"SELECT " +
    	"gene_node.uid AS gene_id, " +
    	"gene_node.label AS gene, " +
    	"COUNT(*) AS annotation_count " +
    	"FROM " +
    	"node AS gene_node " +
    	"JOIN (link AS has_allele_link " +
    	"JOIN (link AS exhibits_link " +
    	"JOIN node AS phenotype_node " +
    	"ON (phenotype_node.node_id = exhibits_link.object_id)) " +
    	"ON (has_allele_link.object_id = exhibits_link.node_id)) " +
    	"ON (gene_node.node_id = has_allele_link.node_id) " +
    	"WHERE " +
    	"exhibits_link.is_inferred = 'f' AND " +
    	"exhibits_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'PHENOSCAPE:exhibits') AND " +
    	"has_allele_link.predicate_id = (SELECT node_id FROM node WHERE uid = 'PHENOSCAPE:has_allele') " +
    	"GROUP BY gene_node.uid, gene_node.label " +
    	"ORDER BY annotation_count DESC";
    
    private String textFileLocation = System.getProperty(TEXT_FILE_LOC) + "/annotationCountByGene.txt";
    
    /**
     * Constructor initializes the shard and the connection to the database
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public GeneAnnotationCounter() throws SQLException, ClassNotFoundException {
		super();
		this.shard = this.initializeShard();
		this.conn = ((AbstractSQLShard)shard).getConnection();
	
	}
    
    /**
     * @PURPOSE The purpose of this method is to get the count of taxon annotations
     * for every anatomical entity and write these counts to a tab delimited text file
     * @throws SQLException
     * @throws IOException
     */
    private void writeAnnotationCountsToFile() 
		throws SQLException, IOException{
    	
    	Statement pStmt  = conn.createStatement();
    	BufferedWriter bw = new BufferedWriter(new FileWriter(new File(textFileLocation)));
    	String entityId, entity, line;
    	int count;

    	ResultSet rs = pStmt.executeQuery(geneCountQuery);
    	while(rs.next()){
    		entityId = rs.getString(1);
    		entity = rs.getString(2);
    		count = rs.getInt(3);
    		line = entityId + "\t\t" + entity + "\t\t" + count + "\n";
			bw.write(line);
    	}

    	bw.flush();
    	bw.close();
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

    /**
     * @PURPOSE This is the main method, which creates an instance of the GAC and invokes the instance method
     * 'writeAnnotationCounts'
     * @param args
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException{
    	GeneAnnotationCounter gac = new GeneAnnotationCounter();
    	gac.writeAnnotationCountsToFile();
    }
}
