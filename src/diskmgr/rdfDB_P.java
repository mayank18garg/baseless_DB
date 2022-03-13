package diskmgr;

import java.io.*;

import bufmgr.*;
import global.*;
import quadrupleheap.*;

public class RDFDB extends DB implements GlobalConst {

    private QuadrupleHeapfile tempQuadrupleHF;        //Temporary heap file for sorting
    private QuadrupleHeapfile quadrupleHF;            //Quadruple heap file to store the quadruples
    private LabelHeapFile entityHF;                   //Entity heap file to store subjects/objects
    private LabelHeapFile predicateHF;                //Predicate heap file to store predicates

    private LabelBTreeFile entityBTree;
    private LabelBTreeFile predicateBTree;
    private QuadrupleBTreeFile quadrupleBTree;

    private String currentDBName;

    private LabelBTreeFile duplicateSubjectTree;
    private LabelBTreeFile duplicateObjectTree;

    private int totalSubjectCount       = 0;
    private int totalObjectCount        = 0;
    private int totalPredicateCount     = 0;
    private int totalEntityCount        = 0;
    private int totalQuadrupleCount     = 0;

    private QuadrupleBTreeFile quadrupleBTreeIndex;

    /**
     * Default constructor
     */
    public RDFDB() {}

    /**
     * Close RDFDB
     */
    public void rdfCloseDB()
            throws 	PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException
    {
        try{

            if( entityBTree != null ) {
                entityBTree.close();
            }
            if( predicateBTree != null ) {
                predicateBTree.close();
            }
            if( quadrupleBTree != null ) {
                quadrupleBTree.close();
            }
            if( duplicateSubjectTree != null ) {
                duplicateSubjectTree.close();
            }
            if( duplicateObjectTree != null ) {
                duplicateObjectTree.close();
            }
            if( quadrupleBTreeIndex != null ) {
                quadrupleBTreeIndex.close();
            }
            //Write one condition for Index file
        }
        catch( Exception e ) {
            e.printStackTrace();
        }

    }

    public void openRDFDB( String dbName, int type ) {
        currentDBName = new String( dbName );
        try{
            openDB( dbName );
            RDFDB( type );
        }
        catch( Exception e ) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

    }

    public void openRDFDB( String dbName, int num_pages, int type ) {
        currentDBName = new String( dbName );
        try{
            openDB( dbName, num_pages );
            RDFDB( type );
        }
        catch( Exception e ) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }

    public void RDFDB( int type ) {

        int keyType = AttrType.attrString;

        PageCounter.initialize();

        try{
            tempQuadrupleHF         = new QuadrupleHeapfile("tempHeapFile");
            quadrupleHF             = new QuadrupleHeapfile( currentDBName + "/quadrupleHeapfile");
            entityHF                = new LabelHeapfile( currentDBName + "/entityHeapfile");
            predicateHF             = new LabelHeapfile( currentDBName + "/predicateHeapfile");
            entityBTree             = new LabelBTreeFile( currentDBName + "/entityBTree", keyType, 255, 1 );
            entityBTree.close();
            predicateBTree          = new LabelBTreeFile( currentDBName + "/predicateBTree", keyType, 255, 1 );
            predicateBTree.close();
            quadrupleBTree          = new QuadrupleBTreeFile( currentDBName + "/quadrupleBTree", keyType, 255, 1 );
            quadrupleBTree.close();
            duplicateSubjectTree    = new LabelBTreeFile( currentDBName + "/duplicateSubjectTree", keyType, 255, 1 );
            duplicateSubjectTree.close();
            duplicateObjectTree     = new LabelBTreeFile( currentDBName + "/duplicateObjectTree", keyType, 255, 1 );
            duplicateObjectTree.close();
            quadrupleBTreeIndex     = new QuadrupleBTreeFile( currentDBName + "/quadrupleBTreeIndex", keyType, 255, 1 );
            quadrupleBTreeIndex.close();

        }
        catch( Exception e ) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

    }

    public int getQuadrupleCnt() {
        try{
            quadrupleHF = new QuadrupleHeapfile( currentDBName + "/quadrupleHeapfile");
            totalQuadrupleCount = quadrupleHF.getQuadrupleCnt();
        }
        catch( Exception e ) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return totalQuadrupleCount;
    }

    public int getPredicateCnt() {
        try{
            predicateHF = new predicateHeapfile( currentDBName + "/predicateHeapfile");
            totalPredicateCount = predicateHF.getPredicateCnt();
        }
        catch( Exception e ){
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return totalPredicateCount;
    }

    public int getEntityCnt() {
        try{
            entityHF = new LabelHeapfile( currentDBName + "/entityHeapfile" );
            totalEntityCount = entityHF.getEntityCnt();
        }
        catch( Exception e ){
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return totalEntityCount;
    }

    public int getSubjectCnt() {
        //TODO
        return totalSubjectCount;
    }

    public int getObjectCnt() {
        //TODO
        return totalObjectCount;
    }


}