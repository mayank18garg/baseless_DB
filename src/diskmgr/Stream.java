package diskmgr;

import heap.InvalidTupleSizeException;
import labelheap.*;
import btree.*;
import global.*;
import quadrupleheap.*;
import iterator.*;

import java.io.IOException;

public class Stream{
    public static String dbName;
    public int sortOption = 0;
    static boolean subjectNull = false;
    static boolean predicateNull = false;
    static boolean objectNull = false;
    static boolean confidenceNull = false;
    public static QuadrupleHeapfile Result_HF = null;
    public QuadrupleSort qsort = null;
    public boolean scanOnBTree = false;
    public TScan iterator = null;
    private boolean scan_entire_heapfile = false;
    private Quadruple scanOnBTreeQuadruple = null;
    private String _subjectFilter;
    private String _predicateFilter;
    private String _objectFilter;
    private double _confidenceFilter;
    public static EID entitySubjectID = new EID();
    public static EID entityObjectID = new EID();
    public static PID predicateID = new PID();

    public QuadrupleOrder get_sort_order()
    {
        QuadrupleOrder sort_order = null;

        switch(sortOption)
        {
            case 1:
                sort_order = new QuadrupleOrder(QuadrupleOrder.SubjectPredicateObjectConfidence);
                break;

            case 2:
                sort_order = new QuadrupleOrder(QuadrupleOrder.PredicateSubjectObjectConfidence);
                break;

            case 3:
                sort_order = new QuadrupleOrder(QuadrupleOrder.SubjectConfidence);
                break;

            case 4:
                sort_order = new QuadrupleOrder(QuadrupleOrder.PredicateConfidence);
                break;

            case 5:
                sort_order = new QuadrupleOrder(QuadrupleOrder.ObjectConfidence);
                break;

            case 6:
                sort_order = new QuadrupleOrder(QuadrupleOrder.Confidence);
                break;
        }
        return sort_order;
    }

    private Quadruple createDummyLastElement()
    {
        PageId pageno = new PageId(-1);

        LID lid = new LID(pageno,-1);
        Quadruple quadruple = new Quadruple();
        try {
            quadruple.setSubjecqid(lid.returnEID());
            quadruple.setPredicateID(lid.returnPID());
            quadruple.setobjecqid(lid.returnEID());
            quadruple.setConfidence(-1);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


        return quadruple;
    }

    //Retrieve next quadruple in the stream
    public Quadruple getNext( QID qid ){
        try{
            Quadruple quadruple = null;
            if(scanOnBTree){
                if(scanOnBTreeQuadruple != null){
                    Quadruple temp = new Quadruple(scanOnBTreeQuadruple);
                    scanOnBTreeQuadruple = null;
                    return temp;
                }
            }
            else{
                if(sortOption == 0)
                    quadruple = iterator.getNext(qid);
                else
                    quadruple = qsort.get_next();
                //System.out.println("qsort qud");
                //System.out.println(quadruple==null);

                while(quadruple != null){
                    //quadruple.print();
                    if(scan_entire_heapfile == false){
                        return quadruple;
                    }
                    else{
                        boolean result = true;
                        Label subject = SystemDefs.JavabaseDB.getEntityHandle().getLabel(quadruple.getSubjecqid().returnLID());
                        Label object = SystemDefs.JavabaseDB.getEntityHandle().getLabel(quadruple.getObjecqid().returnLID());
                        Label predicate = SystemDefs.JavabaseDB.getPredicateHandle().getLabel(quadruple.getPredicateID().returnLID());
                        double confidence = quadruple.getConfidence();

                        //System.out.println(confidence + " " + _confidenceFilter);
                        if(!subjectNull){
                            result = result & (_subjectFilter.compareTo(subject.getLabel()) == 0);
                        }
                        if(!predicateNull){
                            result = result & (_predicateFilter.compareTo(predicate.getLabel()) == 0);
                        }
                        if(!objectNull){
                            result = result & (_objectFilter.compareTo(object.getLabel()) == 0);
                        }
                        if(!confidenceNull){
                            result = result & (quadruple.getConfidence() >= _confidenceFilter);
                        }
                        //System.out.println("result " + result);
                        if(result)
                            return quadruple;
                        quadruple = qsort.get_next();

                    }
                }
            }
        }
        catch(Exception e){
            System.out.println("returning dummy data");
            return createDummyLastElement();
            //e.printStackTrace();
        }
        return null;
    }

    //Close the stream
    public void closeStream(){
        try{
            if(iterator != null)
                iterator.closescan();
            if(Result_HF != null && Result_HF != SystemDefs.JavabaseDB.getQuadrupleHandle())
                Result_HF.deleteFile();
            if(qsort != null)
                qsort.close();
        }
        catch(Exception e){
            System.out.println("Error closing stream");
            e.printStackTrace();
        }
    }

    //Constructor to create a stream object
    public Stream(String rdfDBName, int orderType, String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter, int numbuf) throws Exception {
        sortOption = orderType;
        dbName = rdfDBName;

        //check if any filters are null
        if(subjectFilter==null){
            subjectNull = true;
        }
        if(objectFilter == null){
            objectNull = true;
        }
        if(predicateFilter == null){
            predicateNull = true;
        }
        if(confidenceFilter == 0){
            confidenceNull = true;
        }

        String indexOption = dbName.substring(dbName.lastIndexOf('_') + 1);

        if(!subjectNull && !objectNull && !predicateNull && !confidenceNull){
            scanBTreeIndex(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            scanOnBTree = true;
        }

        else{
            if(Integer.parseInt(indexOption) == 1 && !confidenceNull)
            {
                System.out.println("index 1");
                scanBTConfidenceIndex(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            }
            else if(Integer.parseInt(indexOption) == 2 && !subjectNull && !confidenceNull)
            {
                scanBTSubjectConfidenceIndex(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            }
            else if(Integer.parseInt(indexOption) == 3 && !objectNull && !confidenceNull)
            {
                scanBTObjectConfidenceIndex(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            }
            else if(Integer.parseInt(indexOption) == 4 && !predicateNull && !confidenceNull)
            {
                scanBTPredicateConfidenceIndex(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            }
            else if(Integer.parseInt(indexOption) == 5 && !subjectNull)
            {
                scanBTSubjectIndex(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            }
            else
            {
                System.out.println("scan entire tree");
                scan_entire_heapfile = true;
                scanEntireHeapFile(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            }

            //Sort the results
            iterator = new TScan(Result_HF);
            //printScan();
            if(sortOption != 0){
                try{
                    qsort = new QuadrupleSort(iterator, get_sort_order(), numbuf);
                }
                catch(Exception e){
                    e.printStackTrace();
                }
            }
        }
    }

    public void printScan() throws InvalidTupleSizeException, IOException {
        QID q1 = new QID();
        Quadruple q2 = null;
        System.out.println("printing scan");
        while((q2 = iterator.getNext(q1))!= null)
        {
            q2.print();
        }

        System.out.println("printing scan done");
    }
    public boolean scanBTreeIndex(String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter) throws Exception {
        
        if(getEID(subjectFilter) != null){
            entitySubjectID = getEID(subjectFilter).returnEID();
        }
        else{
			System.out.println("No quadruple found");
			return false;
		}

        if(getPredicateID(predicateFilter) != null){
            predicateID = getPredicateID(predicateFilter).returnPID();
        }
        else{
			System.out.println("No quadruple found");
			return false;
		}

        if(getEID(objectFilter) != null){
            entityObjectID = getEID(objectFilter).returnEID();
        }
        else{
			System.out.println("No quadruple found");
			return false;
		}

        //Compute the key to search the quadruple in the Quadruple BTree
        String key = entitySubjectID.slotNo + ":" + entitySubjectID.pageNo.pid + ":" + predicateID.slotNo + ":" + predicateID.pageNo.pid + ":" 
                        + entityObjectID.slotNo + ":" + entityObjectID.pageNo.pid;
        KeyClass low_key = new StringKey(key);
        KeyClass high_key = new StringKey(key);
        KeyDataEntry entry = null;
        
        QuadrupleBTreeFile QuadrupleBTree = SystemDefs.JavabaseDB.getQuadrupleBT();
        QuadrupleHeapfile QuadrupleHF = SystemDefs.JavabaseDB.getQuadrupleHandle();
        LabelHeapfile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
        LabelHeapfile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();

        QuadrupleBTFileScan scan = QuadrupleBTree.new_scan(low_key, high_key);
        entry = scan.get_next();
        if(entry != null){
            if(key.compareTo(((StringKey)(entry.key)).getKey()) == 0){
                QID quadrupleID = ((QuadrupleLeafData)(entry.data)).getData();
                Quadruple record = QuadrupleHF.getQuadruple(quadrupleID);
                double originalConfidence = record.getConfidence();
                if(originalConfidence > confidenceFilter){
                    scanOnBTreeQuadruple = new Quadruple(record);
                }
            }
        }
        scan.DestroyBTreeFileScan();
        QuadrupleBTree.close();
        return true;
    }

    /**
     * Returns labelID for the given entity
     * @param entityFilter
     * @return
     */
    public static LID getEID(String entityFilter){
        LID eid = null;
        try{
            LabelHeapBTreeFile Entity_BTree = new LabelHeapBTreeFile(dbName+"/entityBT");
            KeyClass low_key = new StringKey(entityFilter);
            KeyClass high_key = new StringKey(entityFilter);
            KeyDataEntry entry = null;
            LabelHeapBTFileScan scan = Entity_BTree.new_scan(low_key, high_key);
            entry = scan.get_next();
            if(entry != null){
                eid = ((LabelHeapLeafData)entry.data).getData();
            }
            else{
                System.out.println("No quadruples found with given subject/object");
            }
            scan.DestroyBTreeFileScan();
            Entity_BTree.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return eid;
    }

    /**
     * Returns predicateID for the given predicate
     * @param predicateFilter
     * @return
     */
    public static LID getPredicateID(String predicateFilter){
        LID predicateID = null;
        try{
            LabelHeapBTreeFile Predicate_BTree = new LabelHeapBTreeFile(dbName+"/predicateBT");
            KeyClass low_key = new StringKey(predicateFilter);
            KeyClass high_key = new StringKey(predicateFilter);
            KeyDataEntry entry = null;
            LabelHeapBTFileScan scan = Predicate_BTree.new_scan(low_key, high_key);
            entry = scan.get_next();
            if(entry != null){
                predicateID = ((LabelHeapLeafData)entry.data).getData();
            }
            else{
                System.out.println("No quadruples found with given predicate");
            }
            scan.DestroyBTreeFileScan();
            Predicate_BTree.close();
        }
        catch(Exception e)
		{
			System.err.println("Predicate not present");
            e.printStackTrace();
		}
        return predicateID;
    }


    private void scanBTConfidenceIndex(String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter) throws Exception {

        boolean result = false;
        KeyDataEntry entry = null;
        QID quadrupleID = null;
        Quadruple record = null;
        Label subject = null, predicate = null, object = null;

        QuadrupleBTreeFile QuadrupleBTree = SystemDefs.JavabaseDB.getQuadrupleBTIndex();
        QuadrupleHeapfile QuadrupleHF = SystemDefs.JavabaseDB.getQuadrupleHandle();
        LabelHeapfile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
        LabelHeapfile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();

        Result_HF = new QuadrupleHeapfile("Result_HF");

        KeyClass low_key = new StringKey(Double.toString(confidenceFilter));
        QuadrupleBTFileScan scan = QuadrupleBTree.new_scan(low_key, null);
        //System.out.println("printing result hf");
        while((entry = scan.get_next()) != null){

            result = true;
            quadrupleID = ((QuadrupleLeafData)entry.data).getData();
            record = QuadrupleHF.getQuadruple(quadrupleID);
            //record.print();
            subject = Entity_HF.getLabel(record.getSubjecqid().returnLID());
            object = Entity_HF.getLabel(record.getObjecqid().returnLID());
            predicate = Predicate_HF.getLabel(record.getPredicateID().returnLID());

            if(!subjectNull){
                result = result && (subjectFilter.compareTo(subject.getLabel()) == 0);
            }
            if(!predicateNull){
                result = result && (predicateFilter.compareTo(predicate.getLabel()) == 0);
            }
            if(!objectNull){
                result = result && (objectFilter.compareTo(object.getLabel()) == 0);
            }
            if(!confidenceNull){
                result = result && (record.getConfidence() >= confidenceFilter);
            }

            if(result){
                Result_HF.insertQuadruple(record.returnQuadrupleByteArray());
            }
        }
       // System.out.println("done print result hf");
       // Result_HF = Result_HF;
        scan.DestroyBTreeFileScan();
        QuadrupleBTree.close();
    }

    private void scanBTSubjectConfidenceIndex(String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter) throws Exception {

        boolean result = false;
        KeyDataEntry entry = null;
        QID quadrupleID = null;
        Quadruple record = null;
        Label subject = null, predicate = null, object = null;

        QuadrupleBTreeFile QuadrupleBTree = SystemDefs.JavabaseDB.getQuadrupleBTIndex();
        QuadrupleHeapfile QuadrupleHF = SystemDefs.JavabaseDB.getQuadrupleHandle();
        LabelHeapfile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
        LabelHeapfile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();

        Result_HF = new QuadrupleHeapfile("Result_HF");

        KeyClass low_key = new StringKey(subjectFilter + ":" + confidenceFilter);
        QuadrupleBTFileScan scan = QuadrupleBTree.new_scan(low_key, null);

        while((entry = scan.get_next()) != null){

            result = true;
            quadrupleID = ((QuadrupleLeafData)entry.data).getData();
            record = QuadrupleHF.getQuadruple(quadrupleID);
            subject = Entity_HF.getLabel(record.getSubjecqid().returnLID());
            object = Entity_HF.getLabel(record.getObjecqid().returnLID());
            predicate = Predicate_HF.getLabel(record.getPredicateID().returnLID());

            if(!subjectNull){
                result = result & (subjectFilter.compareTo(subject.getLabel()) == 0);
            }
            if(!predicateNull){
                result = result & (predicateFilter.compareTo(predicate.getLabel()) == 0);
            }
            if(!objectNull){
                result = result & (objectFilter.compareTo(object.getLabel()) == 0);
            }
            if(!confidenceNull){
                result = result & (record.getConfidence() >= confidenceFilter);
            }
            if(subjectFilter.compareTo(subject.getLabel()) != 0)
                break;

            if(result){
                Result_HF.insertQuadruple(record.returnQuadrupleByteArray());
            }
        }

        scan.DestroyBTreeFileScan();
        QuadrupleBTree.close();
    }

    private void scanBTPredicateConfidenceIndex(String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter) throws Exception {

        boolean result = false;
        KeyDataEntry entry = null;
        QID quadrupleID = null;
        Quadruple record = null;
        Label subject = null, predicate = null, object = null;

        QuadrupleBTreeFile QuadrupleBTree = SystemDefs.JavabaseDB.getQuadrupleBTIndex();
        QuadrupleHeapfile QuadrupleHF = SystemDefs.JavabaseDB.getQuadrupleHandle();
        LabelHeapfile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
        LabelHeapfile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();

        Result_HF = new QuadrupleHeapfile("Result_HF");

        KeyClass low_key = new StringKey(predicateFilter + ":" + confidenceFilter);
        QuadrupleBTFileScan scan = QuadrupleBTree.new_scan(low_key, null);

        while((entry = scan.get_next()) != null){

            result = true;
            quadrupleID = ((QuadrupleLeafData)entry.data).getData();
            record = QuadrupleHF.getQuadruple(quadrupleID);
            subject = Entity_HF.getLabel(record.getSubjecqid().returnLID());
            object = Entity_HF.getLabel(record.getObjecqid().returnLID());
            predicate = Predicate_HF.getLabel(record.getPredicateID().returnLID());

            if(!subjectNull){
                result = result & (subjectFilter.compareTo(subject.getLabel()) == 0);
            }
            if(!predicateNull){
                result = result & (predicateFilter.compareTo(predicate.getLabel()) == 0);
            }
            if(!objectNull){
                result = result & (objectFilter.compareTo(object.getLabel()) == 0);
            }
            if(!confidenceNull){
                result = result & (record.getConfidence() >= confidenceFilter);
            }
            if(predicateFilter.compareTo(predicate.getLabel()) != 0)
                break;

            if(result){
                Result_HF.insertQuadruple(record.returnQuadrupleByteArray());
            }
        }

        scan.DestroyBTreeFileScan();
        QuadrupleBTree.close();
    }

    private void scanBTObjectConfidenceIndex(String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter) throws Exception {

        boolean result = false;
        KeyDataEntry entry = null;
        QID quadrupleID = null;
        Quadruple record = null;
        Label subject = null, predicate = null, object = null;

        QuadrupleBTreeFile QuadrupleBTree = SystemDefs.JavabaseDB.getQuadrupleBTIndex();
        QuadrupleHeapfile QuadrupleHF = SystemDefs.JavabaseDB.getQuadrupleHandle();
        LabelHeapfile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
        LabelHeapfile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();

        Result_HF = new QuadrupleHeapfile("Result_HF");

        KeyClass low_key = new StringKey(objectFilter + ":" + confidenceFilter);
        QuadrupleBTFileScan scan = QuadrupleBTree.new_scan(low_key, null);

        while((entry = scan.get_next()) != null){

            result = true;
            quadrupleID = ((QuadrupleLeafData)entry.data).getData();
            record = QuadrupleHF.getQuadruple(quadrupleID);
            subject = Entity_HF.getLabel(record.getSubjecqid().returnLID());
            object = Entity_HF.getLabel(record.getObjecqid().returnLID());
            predicate = Predicate_HF.getLabel(record.getPredicateID().returnLID());

            if(!subjectNull){
                result = result & (subjectFilter.compareTo(subject.getLabel()) == 0);
            }
            if(!predicateNull){
                result = result & (predicateFilter.compareTo(predicate.getLabel()) == 0);
            }
            if(!objectNull){
                result = result & (objectFilter.compareTo(object.getLabel()) == 0);
            }
            if(!confidenceNull){
                result = result & (record.getConfidence() >= confidenceFilter);
            }
            if(objectFilter.compareTo(object.getLabel()) != 0)
                break;

            if(result){
                Result_HF.insertQuadruple(record.returnQuadrupleByteArray());
            }
        }

        scan.DestroyBTreeFileScan();
        QuadrupleBTree.close();
    }

    private void scanBTSubjectIndex(String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter) throws Exception {

        boolean result = false;
        KeyDataEntry entry = null;
        QID quadrupleID = null;
        Quadruple record = null;
        Label subject = null, predicate = null, object = null;

        QuadrupleBTreeFile QuadrupleBTree = SystemDefs.JavabaseDB.getQuadrupleBTIndex();
        QuadrupleHeapfile QuadrupleHF = SystemDefs.JavabaseDB.getQuadrupleHandle();
        LabelHeapfile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
        LabelHeapfile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();

        Result_HF = new QuadrupleHeapfile("Result_HF");

        KeyClass low_key = new StringKey(subjectFilter);
        KeyClass high_key = new StringKey(subjectFilter);
        QuadrupleBTFileScan scan = QuadrupleBTree.new_scan(low_key, high_key);

        while((entry = scan.get_next()) != null){

            result = true;
            quadrupleID = ((QuadrupleLeafData)entry.data).getData();
            record = QuadrupleHF.getQuadruple(quadrupleID);
            subject = Entity_HF.getLabel(record.getSubjecqid().returnLID());
            object = Entity_HF.getLabel(record.getObjecqid().returnLID());
            predicate = Predicate_HF.getLabel(record.getPredicateID().returnLID());

            if(!predicateNull){
                result = result & (predicateFilter.compareTo(predicate.getLabel()) == 0);
            }
            if(!objectNull){
                result = result & (objectFilter.compareTo(object.getLabel()) == 0);
            }
            if(!confidenceNull){
                result = result & (record.getConfidence() >= confidenceFilter);
            }

            if(result){
                Result_HF.insertQuadruple(record.returnQuadrupleByteArray());
            }
        }

        scan.DestroyBTreeFileScan();
        QuadrupleBTree.close();
    }

    private void scanEntireHeapFile(String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter){
        try{
            _subjectFilter = subjectFilter;
            _predicateFilter = predicateFilter;
            _objectFilter = objectFilter;
            _confidenceFilter = confidenceFilter;
            Result_HF = SystemDefs.JavabaseDB.getQuadrupleHandle();
        }
        catch(Exception e){
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }

}