package iterator;

import global.LID;
import global.QuadrupleOrder;
import global.SystemDefs;
import labelheap.Label;
import labelheap.LabelHeapfile;
import quadrupleheap.Quadruple;

public class QuadrupleUtils {

    /**
     * compare quadruples based on the subject value
     * 
     * @param q1 First quadruple
     * @param q2 Second quadruple
     * @param Entity_HF Entity heapfile to get the subject value
     * @return 
     *  return 1 if q1 is greater than q2
     *  return -1 if q1 is less than q2
     */
    private static int compareSubject(Quadruple q1, Quadruple q2, LabelHeapfile Entity_HF){
        try{
            Label subject1, subject2;
            String q1_subject, q2_subject;
            LID lid1, lid2;
            lid1 = q1.getSubjecqid().returnLID();
            lid2 = q2.getSubjecqid().returnLID();
            char[] c = new char[1];
            c[0] = Character.MIN_VALUE;
            
            if(lid1.pageNo.pid < 0)
                q1_subject = new String(c);
            else{
                subject1 = Entity_HF.getLabel(lid1);
                q1_subject = subject1.getLabel();
            }
            if(lid2.pageNo.pid < 0)
                q2_subject = new String(c);
            else{
                subject2 = Entity_HF.getLabel(lid2);
                q2_subject = subject2.getLabel();
            }
            if(q1_subject.compareTo(q2_subject) > 0)
                return 1;
            if(q1_subject.compareTo(q2_subject) < 0)
                return -1;

            return 0;
        }
        catch(Exception e){
            e.printStackTrace();
            return -2;
        }
    }

    /**
     * compare quadruples based on the predicate value
     * 
     * @param q1 First quadruple
     * @param q2 Second quadruple
     * @param Predicate_HF Predicate heapfile to get the predicate value
     * @return
     *  return 1 if q1 is greater than q2
     *  return -1 if q1 is less than q2
     */
    private static int comparePredicate(Quadruple q1, Quadruple q2, LabelHeapfile Predicate_HF){
        try{
            Label predicate1, predicate2;
            String q1_predicate, q2_predicate;
            LID lid1, lid2;
            lid1 = q1.getPredicateID().returnLID();
            lid2 = q2.getPredicateID().returnLID();
            char[] c = new char[1];
            c[0] = Character.MIN_VALUE;
            
            if(lid1.pageNo.pid < 0)
                q1_predicate = new String(c);
            else{
                predicate1 = Predicate_HF.getLabel(lid1);
                q1_predicate = predicate1.getLabel();
            }
            if(lid2.pageNo.pid < 0)
                q2_predicate = new String(c);
            else{
                predicate2 = Predicate_HF.getLabel(lid2);
                q2_predicate = predicate2.getLabel();
            }
            if(q1_predicate.compareTo(q2_predicate) > 0)
                return 1;
            if(q1_predicate.compareTo(q2_predicate) < 0)
                return -1;

            return 0;
        }
        catch(Exception e){
            e.printStackTrace();
            return -2;
        }
    }

    /**
     * compare quadruples based on the object value
     * 
     * @param q1 First quadruple
     * @param q2 Second quadruple
     * @param Entity_HF Entity heapfile to get the object value
     * @return
     *  return 1 if q1 is greater than q2
     *  return -1 if q1 is less than q2
     */
    private static int compareObject(Quadruple q1, Quadruple q2, LabelHeapfile Entity_HF){
        try{
            Label object1, object2;
            String q1_object, q2_object;
            LID lid1, lid2;
            lid1 = q1.getObjecqid().returnLID();
            lid2 = q2.getObjecqid().returnLID();
            char[] c = new char[1];
            c[0] = Character.MIN_VALUE;
            
            if(lid1.pageNo.pid < 0)
                q1_object = new String(c);
            else{
                object1 = Entity_HF.getLabel(lid1);
                q1_object = object1.getLabel();
            }
            if(lid2.pageNo.pid < 0)
                q2_object = new String(c);
            else{
                object2 = Entity_HF.getLabel(lid2);
                q2_object = object2.getLabel();
            }
            if(q1_object.compareTo(q2_object) > 0)
                return 1;
            if(q1_object.compareTo(q2_object) < 0)
                return -1;

            return 0;
        }
        catch(Exception e){
            e.printStackTrace();
            return -2;
        }
    }

    /**
     * compare quadruples based on confidence value
     * 
     * @param q1 First quadruple
     * @param q2 Second quadruple
     * @return
     *  return 1 if q1 confidence is greater than q2 confidence
     *  return -1 if q1 confidence is less than q2 confidence
     */
    private static int compareConfidence(Quadruple q1, Quadruple q2) {
        try{
            double q1_confidence, q2_confidence;
            q1_confidence = q1.getConfidence();
            q2_confidence = q2.getConfidence();
            if(q1_confidence > q2_confidence)
                return 1;
            if(q1_confidence < q2_confidence)
                return -1;
            
            return 0;
        }
        catch(Exception e){
            e.printStackTrace();
            return -2;
        }
    }

    /**
     * compares one quadruple with other quadruple based on the sort order
     * Sequence of comparisions is given in the orderType
     * 
     * @param orderType The order through which the quadruples are compared
     * @param q1 First quadruple
     * @param q2 Second quadruple
     * @return
     *  return 1 if q1 is greater than q2
     *  return -1 if q1 is less than q2
     */
    public static int CompareQuadrupleWithQuadruple(QuadrupleOrder orderType, Quadruple q1, Quadruple q2){

        LabelHeapfile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
        LabelHeapfile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();
        int retVal = -2;

        //making method calls to functions based on sort orders
        //if the quadruples are equal on one parameter, 
        //continue comparing on other parameters in QuadrupleOrder order
        switch(orderType.qudrupleOrder){
            case QuadrupleOrder.SubjectPredicateObjectConfidence:
                retVal = compareSubject(q1, q2, Entity_HF);
                if(retVal == 0){
                    retVal = comparePredicate(q1, q2, Predicate_HF);
                    if(retVal == 0){
                        retVal = compareObject(q1, q2, Entity_HF);
                        if(retVal == 0){
                            retVal = compareConfidence(q1, q2);
                        }
                    }
                }
                return retVal;

            case QuadrupleOrder.PredicateSubjectObjectConfidence:
                retVal = comparePredicate(q1, q2, Predicate_HF);
                if(retVal == 0){
                    retVal = compareSubject(q1, q2, Entity_HF);
                    if(retVal == 0){
                        retVal = compareObject(q1, q2, Entity_HF);
                        if(retVal == 0){
                            retVal = compareConfidence(q1, q2);
                        }
                    }
                }
                return retVal;

            case QuadrupleOrder.SubjectConfidence:
                retVal = compareSubject(q1, q2, Entity_HF);
                if(retVal == 0){
                    retVal = compareConfidence(q1, q2);
                }
                return retVal;

            case QuadrupleOrder.PredicateConfidence:
                retVal = comparePredicate(q1, q2, Predicate_HF);
                if(retVal == 0){
                    retVal = compareConfidence(q1, q2);
                }
                return retVal;

            case QuadrupleOrder.ObjectConfidence:
                retVal = compareObject(q1, q2, Entity_HF);
                if(retVal == 0){
                    retVal = compareConfidence(q1, q2);
                }
                return retVal;

            case QuadrupleOrder.Confidence:
                retVal = compareConfidence(q1, q2);
                return retVal;

            default:
                return retVal;
        }

    }

    /**
     * set value from one quadruple to other
     * @param quadruple quadruple to copy the values into
     * @param value quadruple to copy the values from
     */
    public static void SetValue(Quadruple quadruple, Quadruple value){
        quadruple.quadrupleCopy(value);
    }
}
