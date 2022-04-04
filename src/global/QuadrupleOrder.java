package global;
/**
 * Enumeration class for QuadrupleOrder
 *
 */
public class QuadrupleOrder {

    public static final int SubjectPredicateObjectConfidence = 1;
    public static final int PredicateSubjectObjectConfidence = 2;
    public static final int SubjectConfidence = 3;
    public static final int PredicateConfidence = 4;
    public static final int ObjectConfidence = 5;
    public static final int Confidence = 6;

    public int qudrupleOrder;

    /**
     * QuadrupleOrder Constructor
     * <br>
     * A Quadruple ordering can be defined as
     * <ul>
     * <li>   QuadrupleOrder quadrupleOrder = new Quadruple(Quadruple.Confidence);
     * </ul>
     * and subsequently used as
     * <ul>
     * <li>   if (quadruple.quadrupleOrder == QuadrupleOrder.Confidence) ....
     * </ul>
     *
     * @param _qudrupleOrder The possible sorting orderType of the triples
     */

	public QuadrupleOrder (int _qudrupleOrder)
    {
        qudrupleOrder = _qudrupleOrder;
    }

    public String toString()
    {
        switch (qudrupleOrder)
        {
            case SubjectPredicateObjectConfidence:
                return "SubjectPredicateObjectConfidence";
            case PredicateSubjectObjectConfidence:
                return "PredicateSubjectObjectConfidence";
            case SubjectConfidence:
                return "SubjectConfidence";
            case PredicateConfidence:
                return "PredicateConfidence";
            case ObjectConfidence:
                return "ObjectConfidence";
            case Confidence:
                return "Confidence";
        }
        return ("Unexpected TripleOrder " + qudrupleOrder);
    }


}
