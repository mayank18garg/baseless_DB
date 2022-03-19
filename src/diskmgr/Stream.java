package diskmgr;

import labelheap.*;
import btree.*;
import bufmgr.*;
import global.*;
import quadrupleheap.*;
import iterator.*;

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
                while(quadruple != null){
                    if(scan_entire_heapfile == false){
                        return quadruple;
                    }
                    else{
                        boolean result = true;
                        Label subject = SystemDefs.JavabaseDB.getEntityHandle().getLabel(quadruple.getSubjecqid().returnLID());
                        Label object = SystemDefs.JavabaseDB.getEntityHandle().getLabel(quadruple.getObjecqid().returnLID());
                        Label predicate = SystemDefs.JavabaseDB.getPredicateHandle().getLabel(quadruple.getPredicateID().returnLID());
                        double confidence = quadruple.getConfidence();

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
                        
                        if(result)
                            return quadruple;

                    }
                }
            }
        }
        catch(Exception e){
            e.printStackTrace();
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
    public Stream(String rdfDBName, int orderType, String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter){
        sortOption = orderType;
        dbName = rdfDBName;

        //check if any filters are null
        if(subjectFilter.compareToIgnoreCase("null") == 0){
            subjectNull = true;
        }
        if(objectFilter.compareToIgnoreCase("null") == 0){
            objectNull = true;
        }
        if(predicateFilter.compareToIgnoreCase("null") == 0){
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
                scan_entire_heapfile = true;
                scanEntireHeapFile(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            }

            //Sort the results
            iterator = new TScan(Result_HF);
            if(sortOption != 0){
                try{
                    qsort = new QuadrupleSort(iterator, sortOption);
                }
                catch(Exception e){
                    e.printStackTrace();
                }
            }
        }
    }

    public boolean scanBTreeIndex(String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter){
        
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
        
        QuadrupleBTreeFile QuadrupleBTree = SystemDefs.JavabaseDB.getQuadruple_BTree();
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


    private void scanBTConfidenceIndex(String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter){

        boolean result = false;
        KeyDataEntry entry = null;
        QID quadrupleID = null;
        Quadruple record = null;
        Label subject = null, predicate = null, object = null;

        QuadrupleBTreeFile QuadrupleBTree = SystemDefs.JavabaseDB.getQuadruple_BTreeIndex();
        QuadrupleHeapfile QuadrupleHF = SystemDefs.JavabaseDB.getQuadrupleHandle();
        LabelHeapfile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
        LabelHeapfile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();

        QuadrupleHeapfile Result_HF = new QuadrupleHeapfile("Result_HF");

        KeyClass low_key = new StringKey(Double.toString(confidenceFilter));
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

            if(result){
                Result_HF.insertQuadruple(record.returnQuadrupleByteArray());
            }
        }

        scan.DestroyBTreeFileScan();
        QuadrupleBTree.close();
    }

    private void scanBTSubjectConfidenceIndex(String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter){

        boolean result = false;
        KeyDataEntry entry = null;
        QID quadrupleID = null;
        Quadruple record = null;
        Label subject = null, predicate = null, object = null;

        QuadrupleBTreeFile QuadrupleBTree = SystemDefs.JavabaseDB.getQuadruple_BTreeIndex();
        QuadrupleHeapfile QuadrupleHF = SystemDefs.JavabaseDB.getQuadrupleHandle();
        LabelHeapfile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
        LabelHeapfile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();

        QuadrupleHeapfile Result_HF = new QuadrupleHeapfile("Result_HF");

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

    private void scanBTPredicateConfidenceIndex(String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter){

        boolean result = false;
        KeyDataEntry entry = null;
        QID quadrupleID = null;
        Quadruple record = null;
        Label subject = null, predicate = null, object = null;

        QuadrupleBTreeFile QuadrupleBTree = SystemDefs.JavabaseDB.getQuadruple_BTreeIndex();
        QuadrupleHeapfile QuadrupleHF = SystemDefs.JavabaseDB.getQuadrupleHandle();
        LabelHeapfile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
        LabelHeapfile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();

        QuadrupleHeapfile Result_HF = new QuadrupleHeapfile("Result_HF");

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

    private void scanBTObjectConfidenceIndex(String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter){

        boolean result = false;
        KeyDataEntry entry = null;
        QID quadrupleID = null;
        Quadruple record = null;
        Label subject = null, predicate = null, object = null;

        QuadrupleBTreeFile QuadrupleBTree = SystemDefs.JavabaseDB.getQuadruple_BTreeIndex();
        QuadrupleHeapfile QuadrupleHF = SystemDefs.JavabaseDB.getQuadrupleHandle();
        LabelHeapfile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
        LabelHeapfile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();

        QuadrupleHeapfile Result_HF = new QuadrupleHeapfile("Result_HF");

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

    private void scanBTSubjectIndex(String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter){

        boolean result = false;
        KeyDataEntry entry = null;
        QID quadrupleID = null;
        Quadruple record = null;
        Label subject = null, predicate = null, object = null;

        QuadrupleBTreeFile QuadrupleBTree = SystemDefs.JavabaseDB.getQuadruple_BTreeIndex();
        QuadrupleHeapfile QuadrupleHF = SystemDefs.JavabaseDB.getQuadrupleHandle();
        LabelHeapfile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
        LabelHeapfile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();

        QuadrupleHeapfile Result_HF = new QuadrupleHeapfile("Result_HF");

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