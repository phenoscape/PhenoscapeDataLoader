package org.phenoscape.obd.loader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.obd.model.CompositionalDescription;
import org.obd.model.CompositionalDescription.Predicate;
import org.obd.model.Graph;
import org.obd.model.LinkStatement;
import org.obd.model.LiteralStatement;
import org.obd.model.Node;
import org.obd.model.Statement;
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
    private Map<Taxon, String> taxonIDToOTUID;
    private Map<Phenotype, String> phenotypeIdMap;
    protected Set<LinkStatement> phenotypes;

    public Graph getGraph() {
        return graph;
    }

    public void setGraph(Graph graph) {
        this.graph = graph;
    }

    public Graph translate(DataSet dataset) throws IOException {
        String dsID = UUID.randomUUID().toString();
        graph = new Graph();
        phenotypes = new HashSet<LinkStatement>();
        characterIdMap = new HashMap<Character, String>();
        stateIdMap = new HashMap<State, String>();
        taxonIdMap = new HashMap<Taxon, String>();
        taxonIDToOTUID = new HashMap<Taxon, String>(); 
        phenotypeIdMap = new HashMap<Phenotype, String>();
        // Dataset metadata
        this.graph.addNode(OBDUtil.createInstanceNode(dsID, Vocab.DATASET_TYPE_ID));
        final String curators = dataset.getCurators();
        LinkStatement ds2p = new LinkStatement(dsID, Vocab.HAS_PUB_REL_ID, dataset.getPublication());
        graph.addStatement(ds2p);

        if (curators != null) {
            LiteralStatement ds2curators = new LiteralStatement(dsID, Vocab.HAS_CURATORS_REL_ID, curators);
            graph.addStatement(ds2curators);
        }
        for (Taxon taxon : dataset.getTaxa()) {
            // avoid uploading taxa without names; Cartik1.0
            //TODO add all taxa as OTUs; check to see if valid name is set before creating annotations
            if ((taxon.getValidName() != null) && (taxon.getValidName().getName() != null) && (taxon.getValidName().getName().length() > 0)) {
                Node taxonNode = translate(taxon);
                if (taxonNode.getId() != null) {
                    taxonIdMap.put(taxon, taxonNode.getId());
                    final String otuID = UUID.randomUUID().toString();
                    final Node otuNode = OBDUtil.createInstanceNode(otuID, Vocab.OTU_TYPE_ID);
                    this.taxonIDToOTUID.put(taxon, otuNode.getId());
                    if (!StringUtils.isBlank(taxon.getPublicationName())) {
                        otuNode.setLabel(taxon.getPublicationName());
                    } else {
                        otuNode.setLabel(taxon.getValidName().getName());    
                    }
                    this.graph.addNode(otuNode);
                    // link dataset to taxa
                    LinkStatement ds2otu = new LinkStatement(dsID, Vocab.HAS_TU_REL_ID, otuNode.getId());
                    this.graph.addStatement(ds2otu);
                    //link otu to taxon
                    LinkStatement otu2t = new LinkStatement(otuID, Vocab.REFERS_TO_TAXON_REL_ID, taxonNode.getId());
                    this.graph.addStatement(otu2t);
                    if (StringUtils.isNotBlank(taxon.getComment())) {
                        LiteralStatement taxonCommentStatement = new LiteralStatement(otuID, Vocab.HAS_COMMENT_REL_ID, taxon.getComment());
                        this.graph.addStatement(taxonCommentStatement);
                    }
                    //link otu to specimens
                    for (Specimen s : taxon.getSpecimens()) {
                        if ((s.getCollectionCode() != null) && (!StringUtils.isBlank(s.getCatalogID()))) {
                            //FIXME should not be relying on toString of Specimen to generate a useful ID for OBD
                            final Node specimenNode = OBDUtil.createInstanceNode(s.toString(), Vocab.SPECIMEN_TYPE_ID);
                            this.graph.addNode(specimenNode);
                            LinkStatement otu2specimen = new LinkStatement(otuID, Vocab.HAS_SPECIMEN_REL_ID, specimenNode.getId());
                            graph.addStatement(otu2specimen);
                            //link specimen to collection 
                            LinkStatement specimen2collection = new LinkStatement(s.toString(), Vocab.SPECIMEN_TO_COLLECTION_REL_ID, 
                                    s.getCollectionCode().getID());
                            graph.addStatement(specimen2collection);
                            //link specimen to catalog id
                            LiteralStatement specimen2catalogId = new LiteralStatement(s.toString(), Vocab.SPECIMEN_TO_CATALOG_ID_REL_ID, s.getCatalogID());
                            graph.addStatement(specimen2catalogId);
                        }
                    }
                }
            }
        }

        // link dataset to characters used in that dataset
        for (Character character : dataset.getCharacters()) {
            // if (character.toString().length() > 0) {
            int charNumber = dataset.getCharacters().indexOf(character) + 1;
            String characterID = UUID.randomUUID().toString();
            final Node characterNode = OBDUtil.createInstanceNode(characterID, Vocab.CHARACTER_TYPE_ID);
            this.graph.addNode(characterNode);
            characterNode.setId(characterID);
            characterNode.setLabel(character.getLabel());
            String charComment = character.getComment();
            if (charComment != null) {
                LiteralStatement chCommentStmt = new LiteralStatement(characterNode.getId(), Vocab.HAS_COMMENT_REL_ID, charComment);
                this.graph.addStatement(chCommentStmt);
            }
            LiteralStatement chNumberStmt = new LiteralStatement(characterNode.getId(), Vocab.HAS_NUMBER_REL_ID, charNumber + "");
            this.graph.addStatement(chNumberStmt);
            characterIdMap.put(character, characterID);
            LinkStatement datasetToCharacterLink = new LinkStatement(dsID, Vocab.HAS_CHARACTER_REL_ID, characterID);
            this.graph.addStatement(datasetToCharacterLink);

            for (State state : character.getStates()) {
                final String stateID = UUID.randomUUID().toString();
                final Node stateNode = OBDUtil.createInstanceNode(stateID, Vocab.STATE_TYPE_ID);
                this.graph.addNode(stateNode);
                stateNode.setLabel(state.getLabel());
                stateNode.setId(stateID);
                String stateComment = state.getComment();
                if (stateComment != null) {
                    LiteralStatement stCommStmt = new LiteralStatement(stateNode.getId(), Vocab.HAS_COMMENT_REL_ID, stateComment);
                    stateNode.addStatement(stCommStmt);
                }
                stateIdMap.put(state, stateID);
                LinkStatement characterToStateLink = new LinkStatement(characterID, Vocab.HAS_STATE_REL_ID, stateID);
                this.graph.addStatement(characterToStateLink);
                this.addAllPhenotypes(state, this.createSymmetricPhenotypes(state));
                for (Phenotype p : state.getPhenotypes()) {
                    CompositionalDescription phenotypeNode = translate(p);
                    if (phenotypeNode != null && phenotypeNode.getId() != null && phenotypeNode.getId().length() > 0) {
                        phenotypeIdMap.put(p, phenotypeNode.getId());
                        LinkStatement s2p = new LinkStatement(stateID, Vocab.HAS_PHENOTYPE_REL_ID, phenotypeNode.getId());
                        this.graph.addStatement(s2p);
                    }
                }
            }
        }

        // Matrix -> annotations
        for (Taxon taxon : dataset.getTaxa()) {
            for (Character character : dataset.getCharacters()) {
                State state = dataset.getStateForTaxon(taxon, character);
                if (state == null) {
                    // System.err.println("no state for t:"+t+" char:"+c);
                    continue;
                }
                for (Phenotype p : state.getPhenotypes()) {
                    // taxon to phenotype
                    LinkStatement annotationLink = new LinkStatement();
                    if (phenotypeIdMap.get(p) != null && taxonIdMap.get(taxon) != null) {
                        annotationLink.setNodeId(taxonIdMap.get(taxon));
                        annotationLink.setTargetId(phenotypeIdMap.get(p));
                        annotationLink.setRelationId(Vocab.TAXON_PHENOTYPE_REL_ID);
                        annotationLink.addSubLinkStatement(Vocab.POSITED_BY_REL_ID, dsID);
                        annotationLink.addSubLinkStatement(Vocab.ANNOTATION_TO_OTU_REL_ID, this.taxonIDToOTUID.get(taxon));
                        // link description of biology back to data
                        final Node cellNode = OBDUtil.createInstanceNode(UUID.randomUUID().toString(), Vocab.CELL_TYPE_ID);
                        this.graph.addNode(cellNode);
                        annotationLink.addSubLinkStatement(Vocab.CELL_TO_STATE_REL_ID, cellNode.getId());
                        phenotypes.add(annotationLink);
                        // cell to state
                        LinkStatement cell2s = new LinkStatement(cellNode.getId(), Vocab.CELL_TO_STATE_REL_ID, stateIdMap.get(state)); 
                        graph.addStatement(cell2s);
                    }
                }
            }
        }
        for (Statement statement : phenotypes) {
            graph.addStatement(statement);
        }
        return graph;
    }

    public CompositionalDescription translate(Phenotype phenotype) throws IOException {
        final OBOClass entity = phenotype.getEntity();
        final OBOClass quality = phenotype.getQuality();
        final OBOClass relatedEntity = phenotype.getRelatedEntity();
        final OBOClass unit = phenotype.getUnit();
        // we are temporarily using the comment field to store count information,
        // which often contains non-integer characters such as > or <
        final String count = phenotype.getComment();
        final Float measurement = phenotype.getMeasurement();
        if (entity == null) {
            return null;
        }
        if (quality == null) {
            return null;
        }
        if (measurement != null && unit == null) {
            return null;
        }
        // cd.addArgument("has_measurement",m);
        final CompositionalDescription phenotypeNode = new CompositionalDescription(Predicate.INTERSECTION);
        phenotypeNode.addArgument(OBDUtil.translateOBOClass(quality));
        // check to avoid a NullPointerException
        if (entity.getParents() != null) {
            phenotypeNode.addArgument(relationVocabulary.inheres_in(), OBDUtil.translateOBOClass(entity));
        }
        if (relatedEntity != null)
            phenotypeNode.addArgument(relationVocabulary.towards(), OBDUtil.translateOBOClass(relatedEntity));
        if (count != null && count.trim().length() > 0){
            phenotypeNode.addArgument(Vocab.HAS_COUNT_REL_ID, count + "");
        }
        if (measurement != null && unit != null){
            phenotypeNode.addArgument(Vocab.HAS_MEASUREMENT_REL_ID, measurement + "");
            phenotypeNode.addArgument(Vocab.HAS_UNIT_REL_ID, unit.getName());
        }
        phenotypeNode.setId(phenotypeNode.generateId());
        getGraph().addStatements(phenotypeNode);
        return phenotypeNode;
    }

    public Node translate(Taxon taxon) {
        Node n = new Node(taxon.getValidName().getID());
        n.setLabel(taxon.getValidName().getName());
        graph.addNode(n);
        return n;
    }
    
    private List<Phenotype> createSymmetricPhenotypes(State state) {
        final List<Phenotype> symmetricPhenotypes = new ArrayList<Phenotype>();
        for (Phenotype phenotype : state.getPhenotypes()) {
            final OBOClass quality = phenotype.getQuality();
            final OBOClass entity = phenotype.getEntity();
            final OBOClass relatedEntity = phenotype.getRelatedEntity();
            if ((relatedEntity != null) && (entity != null) && (quality != null) && (Vocab.SYMMETRIC_QUALITIES.contains(quality.getID()))) {
                symmetricPhenotypes.add(this.createSymmetricPhenotype(phenotype));
            }
        }
        return symmetricPhenotypes;
    }
    
    private Phenotype createSymmetricPhenotype(Phenotype phenotype) {
        final Phenotype symmetricPhenotype = new Phenotype();
        symmetricPhenotype.setEntity(phenotype.getRelatedEntity());
        symmetricPhenotype.setQuality(phenotype.getQuality());
        symmetricPhenotype.setRelatedEntity(phenotype.getEntity());
        symmetricPhenotype.setCount(phenotype.getCount());
        symmetricPhenotype.setComment(phenotype.getComment());
        symmetricPhenotype.setMeasurement(phenotype.getMeasurement());
        symmetricPhenotype.setUnit(phenotype.getUnit());
        return symmetricPhenotype;
    }
    
    private void addAllPhenotypes(State state, List<Phenotype> phenotypes) {
        for (Phenotype phenotype : phenotypes) {
            state.addPhenotype(phenotype);
        }
    }

}
