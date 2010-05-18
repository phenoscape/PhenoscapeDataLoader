package org.phenoscape.obd;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.obd.model.CompositionalDescription;
import org.obd.model.Graph;
import org.obd.model.LinkStatement;
import org.obd.model.LiteralStatement;
import org.obd.model.Node;
import org.obd.model.Statement;
import org.obd.model.CompositionalDescription.Predicate;
import org.obo.datamodel.OBOClass;
import org.phenoscape.model.Character;
import org.phenoscape.model.DataSet;
import org.phenoscape.model.Phenotype;
import org.phenoscape.model.Specimen;
import org.phenoscape.model.State;
import org.phenoscape.model.Taxon;
import org.purl.obo.vocab.RelationVocabulary;

/**
 * Bridges between phenoscape objects and a model expressed using OBO
 * primitives.
 * 
 * OBD Instances and the relations between them generally reflect java instances
 * of the corresponding phenoscape model. We model cells, states and
 * matrixes/datasets using instances of CDAO owl classes. We go beyond the
 * phenoscape model and CDAO in positing an annotation link between the
 * phenotype and the taxon. The annotation is linked to the cell.
 * 
 * TODO: reverse mapping TODO: finalize relations and classes; both from OBO_REL
 * and CDAO TODO: make this subclass a generic bridge framework
 * 
 * @author cjm
 * 
 */
public class OBDModelBridge {

    protected Graph graph;

    private static final RelationVocabulary relationVocabulary = new RelationVocabulary();
    private Map<Character, String> characterIdMap;
    private Map<State, String> stateIdMap;
    private Map<Taxon, String> taxonIdMap;
    private Map<Phenotype, String> phenotypeIdMap;
    protected Set<LinkStatement> phenotypes;

    public Graph getGraph() {
        return graph;
    }

    public void setGraph(Graph graph) {
        this.graph = graph;
    }

    public Graph translate(DataSet ds) throws IOException {
        String dsId = UUID.randomUUID().toString();
        graph = new Graph();
        phenotypes = new HashSet<LinkStatement>();
        characterIdMap = new HashMap<Character, String>();
        stateIdMap = new HashMap<State, String>();
        taxonIdMap = new HashMap<Taxon, String>();
        phenotypeIdMap = new HashMap<Phenotype, String>();
        // Dataset metadata
        this.graph.addNode(OBDUtil.createInstanceNode(dsId, Vocab.DATASET_TYPE_ID));
        final String curators = ds.getCurators();
        final Node pubNode = OBDUtil.createInstanceNode(ds.getPublication(), Vocab.PUBLICATION_TYPE_ID);
        this.graph.addNode(pubNode);
        LinkStatement ds2p = new LinkStatement(dsId, Vocab.HAS_PUB_REL_ID, pubNode.getId());
        graph.addStatement(ds2p);

        if(curators != null){
            LiteralStatement ds2curators = new LiteralStatement(dsId, Vocab.HAS_CURATORS_REL_ID, curators);
            graph.addStatement(ds2curators);
        }
        for (Taxon taxon : ds.getTaxa()) {
            // avoid uploading taxa without names; Cartik1.0
            if (taxon.getValidName() != null && taxon.getValidName().getName() != null
                    && taxon.getValidName().getName().length() > 0) {
                Node tn = translate(taxon);
                if (tn.getId() != null) {
                    taxonIdMap.put(taxon, tn.getId());
                    final String otuId = UUID.randomUUID().toString();
                    final Node otuNode = OBDUtil.createInstanceNode(otuId, Vocab.OTU_TYPE_ID);
                    this.graph.addNode(otuNode);
                    otuNode.setLabel(taxon.getValidName().getName());
                    // link dataset to taxa
                    LinkStatement ds2otu = new LinkStatement(dsId, Vocab.HAS_TU_REL_ID, otuNode.getId());
                    graph.addStatement(ds2otu);
                    //link otu to taxon
                    LinkStatement otu2t = new LinkStatement(otuId,
                            Vocab.REFERS_TO_TAXON_REL_ID, tn.getId());
                    graph.addStatement(otu2t);

                    final String publicationName = taxon.getPublicationName();
                    if (!StringUtils.isBlank(publicationName)) {
                        LiteralStatement otu2pubName = new LiteralStatement(otuId, Vocab.HAS_PUBLICATION_NAME, publicationName);
                        graph.addStatement(otu2pubName);
                    }

                    //link otu to specimens
                    for (Specimen s : taxon.getSpecimens()) {
                        if ((s.getCollectionCode() != null) && (!StringUtils.isBlank(s.getCatalogID()))) {
                            //FIXME should not be relying on toString of Specimen to generate a useful ID for OBD
                            final Node specimenNode = OBDUtil.createInstanceNode(s.toString(), Vocab.SPECIMEN_TYPE_ID);
                            this.graph.addNode(specimenNode);
                            LinkStatement otu2specimen = new LinkStatement(otuId, Vocab.HAS_SPECIMEN_REL_ID, specimenNode.getId());
                            graph.addStatement(otu2specimen);
                            //link specimen to collection 
                            LinkStatement specimen2collection = new LinkStatement(s.toString(), Vocab.SPECIMEN_TO_COLLECTION_REL_ID, 
                                    s.getCollectionCode().getID());
                            graph.addStatement(specimen2collection);
                            //link specimen to catalog id
                            LinkStatement specimen2catalogId = new LinkStatement(s.toString(), Vocab.SPECIMEN_TO_CATALOG_ID_REL_ID, s.getCatalogID());
                            graph.addStatement(specimen2catalogId);
                        }
                    }
                }
            }
        }

        // link dataset to characters used in that dataset
        for (Character character : ds.getCharacters()) {
            // if (character.toString().length() > 0) {
            int charNumber = ds.getCharacters().indexOf(character) + 1;
            String cid = UUID.randomUUID().toString();
            final Node characterNode = OBDUtil.createInstanceNode(cid, Vocab.CHARACTER_TYPE_ID);
            this.graph.addNode(characterNode);
            characterNode.setId(cid);
            characterNode.setLabel(character.getLabel());
            String charComment = character.getComment();
            if(charComment != null){
                LiteralStatement chCommentStmt = 
                    new LiteralStatement(characterNode.getId(), Vocab.HAS_COMMENT_REL_ID, charComment);
                characterNode.addStatement(chCommentStmt);
            }
            LiteralStatement chNumberStmt = 
                new LiteralStatement(characterNode.getId(), Vocab.HAS_NUMBER_REL_ID, charNumber + "");
            characterNode.addStatement(chNumberStmt);
            characterIdMap.put(character, cid);
            LinkStatement ds2c = new LinkStatement(dsId, Vocab.HAS_CHARACTER_REL_ID,
                    cid);
            graph.addStatement(ds2c);

            for (State state : character.getStates()) {
                final String sid = UUID.randomUUID().toString();
                final Node stateNode = OBDUtil.createInstanceNode(sid, Vocab.STATE_TYPE_ID);
                this.graph.addNode(stateNode);
                stateNode.setLabel(state.getLabel());
                stateNode.setId(sid);
                String stateComment = state.getComment();
                if(stateComment != null){
                    LiteralStatement stCommStmt = 
                        new LiteralStatement(stateNode.getId(), Vocab.HAS_COMMENT_REL_ID, stateComment);
                    stateNode.addStatement(stCommStmt);
                }
                stateIdMap.put(state, sid);
                LinkStatement c2s = new LinkStatement(cid, Vocab.HAS_STATE_REL_ID,
                        sid);
                graph.addStatement(c2s);
                for (Phenotype p : state.getPhenotypes()) {
                    CompositionalDescription cd = translate(p);
                    if (cd != null && cd.getId() != null && cd.getId().length() > 0) {
                        phenotypeIdMap.put(p, cd.getId());
                        LinkStatement s2p = new LinkStatement(sid,
                                Vocab.HAS_PHENOTYPE_REL_ID, cd.getId());
                        graph.addStatement(s2p);
                    }
                }
            }
        }

        // Matrix -> annotations
        for (Taxon t : ds.getTaxa()) {
            for (Character c : ds.getCharacters()) {
                State state = ds.getStateForTaxon(t, c);
                if (state == null) {
                    // System.err.println("no state for t:"+t+" char:"+c);
                    continue;
                }
                for (Phenotype p : state.getPhenotypes()) {
                    // taxon to phenotype
                    LinkStatement annotLink = new LinkStatement();
                    if (phenotypeIdMap.get(p) != null && taxonIdMap.get(t) != null) {
                        annotLink.setNodeId(taxonIdMap.get(t));
                        annotLink.setTargetId(phenotypeIdMap.get(p));
                        annotLink.setRelationId(Vocab.TAXON_PHENOTYPE_REL_ID);
                        annotLink.addSubLinkStatement("posited_by", dsId);

                        // link description of biology back to data
                        final Node cellNode = OBDUtil.createInstanceNode(UUID.randomUUID().toString(), Vocab.CELL_TYPE_ID);
                        this.graph.addNode(cellNode);
                        annotLink.addSubLinkStatement(Vocab.CELL_TO_STATE_REL_ID,
                                cellNode.getId());
                        phenotypes.add(annotLink);
                        // cell to state
                        LinkStatement cell2s = new LinkStatement(cellNode.getId(),
                                Vocab.CELL_TO_STATE_REL_ID, stateIdMap.get(state)); 
                        graph.addStatement(cell2s);
                    }
                }
            }
        }
        for (Statement stmt : phenotypes) {
            graph.addStatement(stmt);
        }
        return graph;
    }

    public CompositionalDescription translate(Phenotype p) throws IOException {
        OBOClass e = p.getEntity();
        OBOClass q = p.getQuality();
        OBOClass e2 = p.getRelatedEntity();
        OBOClass u = p.getUnit();
        // we are temporarily using the comment field to store count information,
        // which often contains non-integer characters such as > or <
        String count = p.getComment();
        Float m = p.getMeasurement();
        if (e == null) {
            return null;
        }
        if (q == null) {
            return null;
        }
        if (m != null && u == null) {
            return null;
        }
        // cd.addArgument("has_measurement",m);
        CompositionalDescription cd = new CompositionalDescription(Predicate.INTERSECTION);
        cd.addArgument(q.getID());
        // check to avoid a NullPointerException
        if (e.getParents() != null) {
            cd.addArgument(relationVocabulary.inheres_in(), OBDUtil.translateOBOClass(e));
        }
        if (e2 != null)
            cd.addArgument(relationVocabulary.towards(), OBDUtil.translateOBOClass(e2));
        if(count != null && count.trim().length() > 0){
            cd.addArgument(Vocab.HAS_COUNT_REL_ID, count + "");
        }
        if(m != null && u != null){
            cd.addArgument(Vocab.HAS_MSRMNT_REL_ID, m + "");
            cd.addArgument(Vocab.HAS_UNIT_REL_ID, u.getName());
        }
        cd.setId(cd.generateId());
        getGraph().addStatements(cd);
        return cd;
    }

    public Node translate(Taxon taxon) {
        Node n = new Node(taxon.getValidName().getID());
        n.setLabel(taxon.getValidName().getName());
        graph.addNode(n);
        return n;
    }

}
