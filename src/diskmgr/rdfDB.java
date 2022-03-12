package diskmgr;

import java.io.*;

public class rdfDB extends DB implements GlobalConst {

    private QuadrapleHeapFile tempQuadrapleHF;        //Temporary heap file for sorting
    private QuadrapleHeapFile quadrapleHF;            //Quadraple heap file to store the quadraples
    private LabelHeapFile entityHF;                   //Entity heap file to store subjects/objects
    private LabelHeapFile predicateHF;                //Predicate heap file to store predicates

    private LabelBTreeFile entityBTree;
    private LabelBTreeFile predicateBTree;

    private String current_DBName;
}