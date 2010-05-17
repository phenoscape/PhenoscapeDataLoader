package org.phenoscape.obd;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jdom.Element;
import org.jdom.input.DOMBuilder;
import org.obd.model.Graph;
import org.obd.model.Node;
import org.obd.model.Statement;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class OBDPublicationBridge {
    
    public Graph translate(Document endnoteXML) {
        final Graph graph = new Graph();
        final org.jdom.Document endnoteDoc = new DOMBuilder().build(endnoteXML);
        final Element records = endnoteDoc.getRootElement().getChild("records");
        for (Object recordObj : records.getChildren("record")) {
            final Element record = (Element)recordObj;
            final Graph pubGraph = this.translateRecord(record);
            graph.merge(pubGraph);
        }
        return graph;
    }
    
    public Graph translateRecord(Element record) {
        final Graph pubGraph = new Graph();
        final Element accessionNumElement = record.getChild("accession-num");
        if (accessionNumElement != null) {
            final String pubID = accessionNumElement.getValue().trim();
            final Node pubNode = OBDUtil.createInstanceNode(pubID, Vocab.PUBLICATION_TYPE_ID);
            final Element yearElement = record.getChild("dates").getChild("year");
            String year = null;
            if (yearElement != null) {
                year = yearElement.getValue().trim();
                if (year.length() > 0) {
                    pubGraph.addLiteralStatement(pubNode, Vocab.PUB_HAS_DATE, year);
                } else {
                    log().error("No year for publication: " + pubID);
                }
            } else {
                log().error("No year for publication: " + pubID);
            }
            final List<Author> authors = this.parseAuthors(record.getChild("contributors").getChild("authors"));
            final String pubLabel = this.createAuthorLabel(authors) + " " + year;
            pubNode.setLabel(pubLabel);
            final Element secondaryAuthorsElement = record.getChild("contributors").getChild("secondary-authors");
            final List<Author> secondaryAuthors = new ArrayList<Author>();
            if (secondaryAuthorsElement != null) {
                secondaryAuthors.addAll(this.parseAuthors(secondaryAuthorsElement));
            }
            final String title = record.getChild("titles").getChild("title").getValue().trim(); //TODO handle italics properly
            final Element containerElement = record.getChild("titles").getChild("secondary-title");
            final String containerTitle;
            if (containerElement != null) {
                containerTitle = containerElement.getValue().trim(); //TODO handle italics properly
            } else {
                containerTitle = "";
            }
            final Element abstractElement = record.getChild("abstract");
            if (abstractElement != null) {
                final String abstractText = abstractElement.getValue().trim(); //TODO handle italics properly
                pubGraph.addLiteralStatement(pubNode, Vocab.PUB_HAS_ABSTRACT, abstractText);
            }
            //TODO generate and link full citation
            log().debug("Adding pub: " + pubNode);
            pubGraph.addNode(pubNode);
        } else {
            log().error("Publication has no accession number, skipping record number: " + record.getChildText("rec-number"));
        }
        return pubGraph;
    }
    
    private List<Author> parseAuthors(Element authorsElement) {
        final List<Author> authors = new ArrayList<Author>();
        for (Object authorObj : authorsElement.getChildren("author")) {
            final Element authorElement = (Element)authorObj;
            final String entry = authorElement.getValue();
            if (entry.contains(",")) {
                final String[] components = entry.split(",", 2);
                authors.add(new Author(components[0].trim(), components[1].trim()));
            } else {
                authors.add(new Author(entry.trim(), ""));
            }
        }
        return authors;
    }
    
    private String createAuthorLabel(List<Author> authors) {
        if (authors.isEmpty()) {
            return "";
        } else if (authors.size() == 1) {
            return authors.get(0).getSurname(); 
        } else if (authors.size() == 2) {
            return authors.get(0).getSurname() + " & " + authors.get(1).getSurname();
        } else {
            return authors.get(0).getSurname() + " et al.";
        }
    }
    
    private static class Author {
        
        private final String surname;
        private final String firstName;
        
        public Author(String surname, String firstName) {
            this.surname = surname;
            this.firstName = firstName;
        }
        
        public String getSurname() {
            return this.surname;
        }
        
        public String getFirstName() {
            return this.firstName;
        }
    }

    private Logger log() {
        return Logger.getLogger(this.getClass());
    }
    
    /**
     * This main is simply for interactive testing.
     */
    public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException {
        Logger.getRootLogger().setLevel(Level.ALL);
        final DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
        final DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
        final Document doc = docBuilder.parse(new File("/Users/jim/Downloads/Phenoscape_pubs_A_papers.xml"));
        final Graph graph = new OBDPublicationBridge().translate(doc);
        for (Statement statement : graph.getAllStatements()) {
            System.out.println(statement);
        }
    }
    
}
