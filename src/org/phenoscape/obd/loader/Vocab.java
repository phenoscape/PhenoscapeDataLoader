package org.phenoscape.obd.loader;

import java.util.Arrays;
import java.util.List;

public class Vocab {

    public static final String PUB_HAS_DATE = "dc:date";
    public static final String PUB_HAS_ABSTRACT = "dc:abstract";
    public static final String PUB_HAS_CITATION = "dc:bibliographicCitation";
    public static final String PUB_HAS_DOI = "dc:identifier";
    public static final String PUBLICATION_TYPE_ID = "PHENOSCAPE:Publication";
    public static final String PHENOSCAPE_PUB_NAMESPACE = "phenoscape_pub";
    public static final String ZFIN_PUB_NAMESPACE = "zfin_pub";
    public static final String DATASET_TYPE_ID = "cdao:CharacterStateDataMatrix";
    public static final String STATE_TYPE_ID = "cdao:CharacterStateDomain";
    public static final String CELL_TYPE_ID = "cdao:CharacterStateDatum";
    public static final String CHARACTER_TYPE_ID = "cdao:Character";
    public static final String OTU_TYPE_ID = "cdao:TU";
    public static final String SPECIMEN_TYPE_ID = "PHENOSCAPE:Specimen";
    public static final String HAS_PUB_REL_ID = "PHENOSCAPE:has_publication";
    public static final String HAS_SPECIMEN_REL_ID = "dwc:individualID";
    public static final String HAS_STATE_REL_ID = "cdao:has_Datum";
    public static final String REFERS_TO_TAXON_REL_ID = "PHENOSCAPE:has_taxon";
    public static final String ANNOTATION_TO_OTU_REL_ID = "PHENOSCAPE:asserted_for_otu";
    public static final String HAS_TU_REL_ID = "cdao:has_TU";
    public static final String HAS_CHARACTER_REL_ID = "cdao:has_Character";
    public static final String HAS_PHENOTYPE_REL_ID = "cdao:has_Phenotype";
    public static final String TAXON_PHENOTYPE_REL_ID = "PHENOSCAPE:exhibits";
    public static final String CELL_TO_STATE_REL_ID = "cdao:has_State";
    public static final String ANNOT_TO_CELL_REL_ID = "PHENOSCAPE:has_source";
    public static final String SPECIMEN_TO_COLLECTION_REL_ID = "dwc:collectionID";
    public static final String SPECIMEN_TO_CATALOG_ID_REL_ID = "dwc:catalogID";
    public static final String HAS_CURATORS_REL_ID = "PHENOSCAPE:has_curators";
    public static final String HAS_COMMENT_REL_ID = "PHENOSCAPE:has_comment";
    public static final String HAS_NUMBER_REL_ID = "PHENOSCAPE:has_number";
    public static final String HAS_COUNT_REL_ID = "PHENOSCAPE:has_count";
    public static final String HAS_MEASUREMENT_REL_ID = "PHENOSCAPE:has_measurement";
    public static final String HAS_UNIT_REL_ID = "PHENOSCAPE:has_unit";
    public static final String GENOTYPE_PHENOTYPE_REL_ID = "OBO_REL:influences";
    public static final String GENOTYPE_GENE_REL_ID = "OBO_REL:variant_of";
    public static final String GENOTYPE_TYPE_ID = "SO:0001027";
    public static final String GENE_TYPE_ID = "SO:0000704";
    public static final String MORPHOLINO_OLIGO = "SO:0000034";
    public static final String POSITED_BY_REL_ID = "posited_by";
    public static final String GENE_NAMESPACE = "zfin_gene";
    public static final String FULL_NAME_SYNONYM_CATEGORY = "FULLNAME";
    public static final String HAS_DBXREF = "oboInOwl:hasDbXref";
    public static final String GENE_TO_CELLULAR_COMPONENT_REL_ID = "OBO_REL:located_in";
    public static final String GENE_TO_MOLECULAR_FUNCTION_REL_ID = "OBO_REL:has_function";
    public static final String GENE_TO_BIOLOGICAL_PROCESS_REL_ID = "OBO_REL:participates_in";
    public static final String PATO_ABSENT = "PATO:0000462";
    
    public static final String ARTICULATED_WITH = "PATO:0002278";
    public static final String ASSOCIATED_WITH = "PATO:0001668";
    public static final String ATTACHED_TO = "PATO:0001667";
    public static final String DETACHED_FROM = "PATO:0001453";
    public static final String DISSOCIATED_FROM = "PATO:0001738";
    public static final String FUSED_WITH = "PATO:0000642";
    public static final String IN_CONTACT_WITH = "PATO:0001961";
    public static final String OVERLAP_WITH = "PATO:0001590";
    public static final String SEPARATED_FROM = "PATO:0001505";
    public static final String UNFUSED_FROM = "PATO:0000651";
    public static final String STRUCTURE = "PATO:0000141";
    public static final List<String> SYMMETRIC_QUALITIES = Arrays.asList(ARTICULATED_WITH, ASSOCIATED_WITH, ATTACHED_TO, DETACHED_FROM, DISSOCIATED_FROM, FUSED_WITH, IN_CONTACT_WITH, OVERLAP_WITH, SEPARATED_FROM, UNFUSED_FROM, STRUCTURE);
    
}
