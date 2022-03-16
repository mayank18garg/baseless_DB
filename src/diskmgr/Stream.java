package diskmgr;

import labelheap.*;
import btree.*;
import bufmgr.*;
import global.*;
import quadrupleheap.*;
import quadrupleiterator.*;

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

    //Implement sortOrder function after the iterator is done

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
                        Label subject = SystemDefs.JavabaseDB.getEntity_HF().getLabel(quadruple.getSubjectID().returnLID());
                        Label object = SystemDefs.JavabaseDB.getEntity_HF().getLabel(quadruple.getObjectID().returnLID());
                        Label predicate = SystemDefs.JavabaseDB.getPredicate_HF.getLabel(record.getPredicateID().returnLID());
                        double confidence = quadruple.getConfidence();

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

    public void closeStream(){
        try{
            if(iterator != null)
                iterator.closescan();
            if(Result_HF != null && Result_HF != SystemDefs.JavabaseDB.getQuadrupleHF())
                Result_HF.deleteFile();
            if(qsort != null)
                qsort.close();
        }
        catch(Exception e){
            System.out.println("Error closing stream");
            e.printStackTrace();
        }
    }

    public Stream(String rdfDBName, int orderType, String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter){
        sortOption = orderType;
        dbName = rdfDBName;

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
            if(Integer.parseInt(indexoption) == 1 && !confidence_null)
            {
                scanBTConfidenceIndex(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            }
            else if(Integer.parseInt(indexoption) == 2 && !subject_null && !confidence_null)
            {
                scanBTSubjectConfidenceIndex(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            }
            else if(Integer.parseInt(indexoption) == 3 && !object_null && !confidence_null)
            {
                scanBTObjectConfidenceIndex(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            }
            else if(Integer.parseInt(indexoption) == 4 && !predicate_null && !confidence_null)
            {
                scanBTPrediateConfidenceIndex(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            }
            else if(Integer.parseInt(indexoption) == 5 && !subject_null)
            {
                scanBTSubjectIndex(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            }
            else
            {	
                scan_entire_heapfile = true;
                scanEntireHeapFile(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            }
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
        
        if(getEID(subject) != null){
            entitySubjectID = getEID(subject).returnEID();
        }
        else{
			System.out.println("No triple found");
			return false;
		}

        if(getPredicate(predicate) != null){
            predicateID = getPredicateID(predicate).returnPredicateID();
        }
        else{
			System.out.println("No triple found");
			return false;
		}

        if(getEID(object) != null){
            entityObjectID = getEID(object).returnEID();
        }
        else{
			System.out.println("No triple found");
			return false;
		}

        String key = entitySubjectID.slotNo + ":" + entitySubjectID.pageNo.pid + ":" + predicateID.slotNo + ":" + predicateID.pageNo.pid + ":" 
                        + entityObjectID.slotNo + ":" + entityObjectID.pageNo.pid;
        KeyClass low_key = new StringKey(key);
        KeyClass high_key = new StringKey(key);
        KeyDataEntry entry = null;
        
        QuadrupleBTreeFile QuadrupleBTree = SystemDefs.JavabaseDB.getQuadrupleBTree();
        QuadrupleHeapfile QuadrupleHF = SystemDefs.JavabaseDB.getQuadrupleHF();
        LabelHeapfile Entity_HF = SystemDefs.JavabaseDB.getEntity_HF();
        LabelHeapfile Predicate_HF = SystemDefs.JavabaseDB.getPredicate_HF();

        QuadrupleBTFileScan scan = QuadrupleBTree.new_scan(low_key, high_key);
        entry = scan.get_next();
        if(entry != null){
            if(key.compareTo(((StringKey)(entry.key)).getKey) == 0){
                QID quadrupleID = ((QuadrupleLeafData)(entry.data)).getData();
                Quadruple record = QuadrupleHF.getQuadruple(quadrupleID);
                double originalConfidence = record.getConfidence();
                if(originalConfidence > confidence){
                    scanOnBTreeQuadruple = new Quadruple(record);
                }
            }
        }
        scan.DestroyBTreeFileScan();
        QuadrupleBTree.close();
        return true;
    }

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
                eid = ((LabelLeafData)entry.data).getData();
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
                predicateID = ((LabelLeafData)entry.data).getData();
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

        QuadrupleBTreeFile QuadrupleBTree = SystemDefs.JavabaseDB.getQuadrupleBTreeIndex();
        QuadrupleHeapfile QuadrupleHF = SystemDefs.JavabaseDB.getQuadrupleHF();
        LabelHeapfile Entity_HF = SystemDefs.JavabaseDB.getEntity_HF();
        LabelHeapfile Predicate_HF = SystemDefs.JavabaseDB.getPredicate_HF();

        QuadrupleHeapfile Result_HF = new QuadrupleHeapfile("Result_HF");

        KeyClass low_key = new StringKey(Double.toString(confidenceFilter));
        QuadrupleBTFileScan scan = QuadrupleBTree.new_scan(low_key, null);

        while((entry.get_next()) != null){

            result = true;
            quadrupleID = ((QuadrupleLeafData)entry.data).getData();
            record = QuadrupleHF.getQuadruple(quadrupleID);
            subject = Entity_HF.getLabel(record.getSubjectID().returnLID());
            object = Entity_HF.getLabel(record.getObjectID().returnLID());
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

        QuadrupleBTreeFile QuadrupleBTree = SystemDefs.JavabaseDB.getQuadrupleBTreeIndex();
        QuadrupleHeapfile QuadrupleHF = SystemDefs.JavabaseDB.getQuadrupleHF();
        LabelHeapfile Entity_HF = SystemDefs.JavabaseDB.getEntity_HF();
        LabelHeapfile Predicate_HF = SystemDefs.JavabaseDB.getPredicate_HF();

        QuadrupleHeapfile Result_HF = new QuadrupleHeapfile("Result_HF");

        KeyClass low_key = new StringKey(subjectFilter + ":" + confidenceFilter);
        QuadrupleBTFileScan scan = QuadrupleBTree.new_scan(low_key, null);

        while((entry.get_next()) != null){

            result = true;
            quadrupleID = ((QuadrupleLeafData)entry.data).getData();
            record = QuadrupleHF.getQuadruple(quadrupleID);
            subject = Entity_HF.getLabel(record.getSubjectID().returnLID());
            object = Entity_HF.getLabel(record.getObjectID().returnLID());
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

        QuadrupleBTreeFile QuadrupleBTree = SystemDefs.JavabaseDB.getQuadrupleBTreeIndex();
        QuadrupleHeapfile QuadrupleHF = SystemDefs.JavabaseDB.getQuadrupleHF();
        LabelHeapfile Entity_HF = SystemDefs.JavabaseDB.getEntity_HF();
        LabelHeapfile Predicate_HF = SystemDefs.JavabaseDB.getPredicate_HF();

        QuadrupleHeapfile Result_HF = new QuadrupleHeapfile("Result_HF");

        KeyClass low_key = new StringKey(predicateFilter + ":" + confidenceFilter);
        QuadrupleBTFileScan scan = QuadrupleBTree.new_scan(low_key, null);

        while((entry.get_next()) != null){

            result = true;
            quadrupleID = ((QuadrupleLeafData)entry.data).getData();
            record = QuadrupleHF.getQuadruple(quadrupleID);
            subject = Entity_HF.getLabel(record.getSubjectID().returnLID());
            object = Entity_HF.getLabel(record.getObjectID().returnLID());
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

        QuadrupleBTreeFile QuadrupleBTree = SystemDefs.JavabaseDB.getQuadrupleBTreeIndex();
        QuadrupleHeapfile QuadrupleHF = SystemDefs.JavabaseDB.getQuadrupleHF();
        LabelHeapfile Entity_HF = SystemDefs.JavabaseDB.getEntity_HF();
        LabelHeapfile Predicate_HF = SystemDefs.JavabaseDB.getPredicate_HF();

        QuadrupleHeapfile Result_HF = new QuadrupleHeapfile("Result_HF");

        KeyClass low_key = new StringKey(objectFilter + ":" + confidenceFilter);
        QuadrupleBTFileScan scan = QuadrupleBTree.new_scan(low_key, null);

        while((entry.get_next()) != null){

            result = true;
            quadrupleID = ((QuadrupleLeafData)entry.data).getData();
            record = QuadrupleHF.getQuadruple(quadrupleID);
            subject = Entity_HF.getLabel(record.getSubjectID().returnLID());
            object = Entity_HF.getLabel(record.getObjectID().returnLID());
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

        QuadrupleBTreeFile QuadrupleBTree = SystemDefs.JavabaseDB.getQuadrupleBTreeIndex();
        QuadrupleHeapfile QuadrupleHF = SystemDefs.JavabaseDB.getQuadrupleHF();
        LabelHeapfile Entity_HF = SystemDefs.JavabaseDB.getEntity_HF();
        LabelHeapfile Predicate_HF = SystemDefs.JavabaseDB.getPredicate_HF();

        QuadrupleHeapfile Result_HF = new QuadrupleHeapfile("Result_HF");

        KeyClass low_key = new StringKey(subjectFilter);
        KeyClass high_key = new StringKey(subjectFilter);
        QuadrupleBTFileScan scan = QuadrupleBTree.new_scan(low_key, high_key);

        while((entry.get_next()) != null){

            result = true;
            quadrupleID = ((QuadrupleLeafData)entry.data).getData();
            record = QuadrupleHF.getQuadruple(quadrupleID);
            subject = Entity_HF.getLabel(record.getSubjectID().returnLID());
            object = Entity_HF.getLabel(record.getObjectID().returnLID());
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
            Result_HF = SystemDefs.JavabaseDB.getQuadrupleHF();
        }
        catch(Exception e){
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }

}