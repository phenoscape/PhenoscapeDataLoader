package org.phenoscape.obd.loader;

import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;

public class SolrClearer {

    public static final String SOLR_URL = "solr-url";
    private SolrServer solr;

    private SolrServer getSolrServer() throws MalformedURLException {
        return new CommonsHttpSolrServer(System.getProperty(SOLR_URL));
    }

    public void clearSolrIndex() throws SolrServerException, IOException {
        this.solr = this.getSolrServer();
        this.solr.deleteByQuery("*:*");
    }

    public static void main(String[] args) throws SolrServerException, IOException {
        //      Logger.getRootLogger().setLevel(Level.ALL);
        //      final Properties properties = new Properties();
        //      properties.load(SolrPhenotypeLoader.class.getResourceAsStream("connection.properties"));
        //      for (Entry<Object, Object> entry : properties.entrySet()) {
        //          System.setProperty(entry.getKey().toString(), entry.getValue().toString());
        //      }

        final SolrClearer clearer = new SolrClearer();
        clearer.clearSolrIndex();
    }

}
