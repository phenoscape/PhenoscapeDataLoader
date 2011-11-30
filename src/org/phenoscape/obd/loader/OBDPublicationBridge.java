package org.phenoscape.obd.loader;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jdom.Element;
import org.jdom.input.DOMBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import org.jdom.transform.XSLTransformException;
import org.jdom.transform.XSLTransformer;
import org.obd.model.Graph;
import org.obd.model.Node;
import org.obd.model.NodeAlias;
import org.obd.model.NodeAlias.Scope;
import org.obd.model.Statement;
import org.xml.sax.SAXException;

public class OBDPublicationBridge {

    private static final String ITALICS_XSLT =  "<?xml version=\"1.0\" encoding=\"UTF-8\" ?> <xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\"> <xsl:output encoding=\"UTF-8\" indent=\"yes\" method=\"xml\" /> <xsl:template match=\"/\"> <xsl:apply-templates/> </xsl:template> <xsl:template match=\"style[@face='italic']\"> <i><xsl:apply-templates/></i> </xsl:template> </xsl:stylesheet> ";

    public Graph translate(org.w3c.dom.Document endnoteXML) throws XSLTransformException {
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

    public Graph translateRecord(Element record) throws XSLTransformException {
        final Graph pubGraph = new Graph();
        final Element accessionNumElement = record.getChild("accession-num");
        if (accessionNumElement != null) {
            final String pubID = accessionNumElement.getValue().trim();
            final Node pubNode = OBDUtil.createInstanceNode(pubID, Vocab.PUBLICATION_TYPE_ID);
            pubNode.setSourceId(Vocab.PHENOSCAPE_PUB_NAMESPACE);
            final Element yearElement = record.getChild("dates").getChild("year");
            final String year;
            if (yearElement != null) {
                year = yearElement.getValue().trim();
                if (year.length() > 0) {
                    pubGraph.addLiteralStatement(pubNode, Vocab.PUB_HAS_DATE, year);
                } else {
                    log().error("No year for publication: " + pubID);
                }
            } else {
                year = null;
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
            final XSLTransformer transformer = new XSLTransformer(new StringReader(ITALICS_XSLT));
            final Format format = Format.getCompactFormat();
            format.setOmitDeclaration(true);
            final Element titleElement = record.getChild("titles").getChild("title");
            final List<?> transformedTitle = transformer.transform(titleElement.getChildren());
            final String title = ((new XMLOutputter()).outputString(transformedTitle)).trim();
            final Element containerElement = record.getChild("titles").getChild("secondary-title");
            final String containerTitle;
            if (containerElement != null) {
                containerTitle = containerElement.getValue().trim(); //TODO handle italics properly?
            } else {
                containerTitle = "";
            }
            final Element volumeElement = record.getChild("volume");
            final String volumeText;
            if (volumeElement != null) {
                volumeText = volumeElement.getValue().trim();
            } else {
                volumeText = "";
            }
            final Element pagesElement = record.getChild("pages");
            final String pagesText;
            if (pagesElement != null) {
                pagesText = pagesElement.getValue().trim();
            } else {
                pagesText = "";
            }
            final Element abstractElement = record.getChild("abstract");
            if (abstractElement != null) {
                final List<?> transformedAbstract = transformer.transform(abstractElement.getChildren());
                final String abstractText = ((new XMLOutputter()).outputString(transformedAbstract)).trim();
                pubGraph.addLiteralStatement(pubNode, Vocab.PUB_HAS_ABSTRACT, abstractText);
            }
            final String fullCitation = this.createFullCitation(authors, year, title, containerTitle, volumeText, pagesText);
            pubGraph.addLiteralStatement(pubNode, Vocab.PUB_HAS_CITATION, fullCitation);
            final NodeAlias fullCitationAsSynonym = new NodeAlias(pubID, fullCitation);
            fullCitationAsSynonym.setScope(Scope.EXACT);
            pubGraph.addStatement(fullCitationAsSynonym);
            final Element doiElement = record.getChild("electronic-resource-num");
            if (doiElement != null) {
                final String doi = doiElement.getValue().trim();
                pubGraph.addLiteralStatement(pubNode, Vocab.PUB_HAS_DOI, doi);
            }
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

    /**
     * Create a short form of the author list for use in an abbreviated label for the publication.
     */
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

    /**
     * Create a full author list for use in a bibliographic citation for the publication.
     */
    private String createCitationAuthors(List<Author> authors) {
        if (authors.isEmpty()) {
            return "";
        } else {
            final StringBuffer sb = new StringBuffer();
            final Iterator<Author> authorsIterator = authors.iterator();
            boolean first = true;
            while (authorsIterator.hasNext()) {
                final Author author = authorsIterator.next();
                if (first) {
                    first = false;
                    sb.append(author.getSurname() + ", " + author.getFirstName());
                } else {
                    if (authorsIterator.hasNext()) { 
                        sb.append(", ");
                    } else {
                        if (authors.size() > 2) { sb.append(","); }
                        sb.append(" and ");
                    }
                    sb.append(author.getFirstName() + " " + author.getSurname());
                }
            }
            return sb.toString();
        }
    }

    private String createFullCitation(List<Author> authors, String year, String title, String containerTitle, String volume, String pages) {
        //TODO this could probably be a little smarter
        String citationAuthors = this.createCitationAuthors(authors);
        if (!citationAuthors.endsWith(".")) {
            citationAuthors += "."; 
        }
        return String.format("%s %s. %s. %s %s%s%s", citationAuthors, year, title, containerTitle, volume, ((!StringUtils.isBlank(volume) && !StringUtils.isBlank(pages)) ? ":" : ""), pages);
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
     * @throws XSLTransformException 
     */
    public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException, XSLTransformException {
        Logger.getRootLogger().setLevel(Level.DEBUG);
        final DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
        final DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
        final org.w3c.dom.Document doc = docBuilder.parse(new File("/Users/jim/Work/Phenoscape/SVN/data/publications/Phenoscape_pubs_A_papers.xml"));
        final Graph graph = new OBDPublicationBridge().translate(doc);
        for (Statement statement : graph.getAllStatements()) {
            System.out.println(statement);
        }
    }

}
