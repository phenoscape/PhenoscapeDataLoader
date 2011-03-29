package org.phenoscape.obd.loader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.obd.model.Graph;
import org.obd.model.LinkStatement;
import org.obd.model.Statement;
import org.obd.query.impl.OBDSQLShard;

public class ZFINGOAnnotationsLoader {
    
    /** The db-host system property should contain the name of the database server. */
    public static final String DB_HOST = "db-host";
    /** The db-name system property should contain the name of the database. */
    public static final String DB_NAME = "db-name";
    /** The db-user system property should contain the database username. */
    public static final String DB_USER = "db-user";
    /** The db-password system property should contain the database password. */
    public static final String DB_PASSWORD = "db-password";
    /** The go-annotations-url system property should contain the URL of the ZFIN GO annotations file. */
    public static final String ANNOTATIONS_URL = "go-annotations-url";
    private static Map<String, String> GO_RELATIONS = new HashMap<String, String>();
    static {
        GO_RELATIONS.put("P", Vocab.GENE_TO_BIOLOGICAL_PROCESS_REL_ID);
        GO_RELATIONS.put("F", Vocab.GENE_TO_MOLECULAR_FUNCTION_REL_ID);
        GO_RELATIONS.put("C", Vocab.GENE_TO_CELLULAR_COMPONENT_REL_ID);
    }

    public void loadAnnotationsData() throws IOException, SQLException, ClassNotFoundException {
        final OBDSQLShard shard = this.initializeShard();
        final Graph graph = new Graph();
        final BufferedReader reader = this.getPublicationsData();
        String line;
        while ((line = reader.readLine()) != null) {
            graph.addStatement(this.parseAnnotationLine(line));
        }
        shard.putGraph(graph);
        shard.disconnect();
    }

    private OBDSQLShard initializeShard() throws SQLException, ClassNotFoundException {
        OBDSQLShard shard = new OBDSQLShard();
        shard.connect("jdbc:postgresql://" + System.getProperty(DB_HOST) + "/" + System.getProperty(DB_NAME), System.getProperty(DB_USER), System.getProperty(DB_PASSWORD));
        return shard;
    }
    
    private BufferedReader getPublicationsData() throws IOException {
        final URL annotationsURL = new URL(System.getProperty(ANNOTATIONS_URL));
        return new BufferedReader(new InputStreamReader(new GZIPInputStream(annotationsURL.openStream())));
    }
    
    private Statement parseAnnotationLine(String line) {
        final String[] items = line.split("\\t", -1);
        final String geneIDEntry = StringUtils.trimToNull(this.getItem(items, 1));
        final String goID = StringUtils.trimToNull(this.getItem(items, 4));
        final String goAspect = StringUtils.trimToNull(this.getItem(items, 8));
        final String relation = GO_RELATIONS.get(goAspect);
        if ((geneIDEntry != null) && (goID != null) && (relation != null)) {
            final String geneID = "ZFIN:" + geneIDEntry;
            return new LinkStatement(geneID, relation, goID);
        }
        return null;
    }
    
    private String getItem(String[] items, int index) {
        return (index < items.length) ? items[index].trim() : null;
    }
    
    @SuppressWarnings("unused")
    private Logger log() {
        return Logger.getLogger(this.getClass());
    }
    
    /**
     * For testing only.
     * @param args
     * @throws ClassNotFoundException 
     * @throws SQLException 
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
        Logger.getRootLogger().setLevel(Level.ALL);
        final Properties properties = new Properties();
        properties.load(SolrPhenotypeLoader.class.getResourceAsStream("connection.properties"));
        for (Entry<Object, Object> entry : properties.entrySet()) {
            System.setProperty(entry.getKey().toString(), entry.getValue().toString());
        }
        final ZFINGOAnnotationsLoader loader = new ZFINGOAnnotationsLoader();
        loader.loadAnnotationsData();
    }

}
