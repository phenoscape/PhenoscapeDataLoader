package org.phenoscape.obd.loader;

import java.util.Collection;

import org.obd.model.CompositionalDescription;
import org.obd.model.LinkStatement;
import org.obd.model.Node;
import org.obd.model.CompositionalDescription.Predicate;
import org.obd.model.Node.Metatype;
import org.obo.datamodel.Link;
import org.obo.datamodel.OBOClass;
import org.obo.util.ReasonerUtil;
import org.obo.util.TermUtil;
import org.purl.obo.vocab.RelationVocabulary;

public class OBDUtil {
    
    private static final RelationVocabulary vocab = new RelationVocabulary();

    public static CompositionalDescription translateOBOClass(OBOClass c) {
        if (TermUtil.isIntersection(c)) {
            final CompositionalDescription cd = new CompositionalDescription(Predicate.INTERSECTION);
            cd.setId(c.getID());
            final OBOClass g = ReasonerUtil.getGenus(c);
            cd.addArgument(translateOBOClass(g));
            final Collection<Link> diffs = ReasonerUtil.getDifferentia(c);
            for (Link diff : diffs) {
                cd.addArgument(diff.getType().getID(), translateOBOClass((OBOClass) diff.getParent()));
            }
            return cd;
        } else {
            final CompositionalDescription d = new CompositionalDescription(Predicate.ATOM);
            d.setNodeId(c.getID());
            return d;
        }
    }
    
    public static Node createInstanceNode(String id, String typeID) {
        final Node n = new Node(id);
        n.setMetatype(Metatype.CLASS);  //why is this CLASS? should it be INSTANCE??
        n.addStatement(new LinkStatement(id, vocab.instance_of(), typeID));
        return n;
    }
    
}
