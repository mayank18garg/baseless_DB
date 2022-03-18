package tests;

import diskmgr.PCounter;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import labelheap.LabelHeapfile;
import quadrupleheap.Quadruple;
import quadrupleheap.QuadrupleHeapfile;

import java.io.IOException;

public class Report {

    static SystemDefs sysdef;
    static LabelHeapfile EntityHF, PredicateHF;
    static QuadrupleHeapfile QuadrupleHF;

    public static void main(String[] args)
            throws HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, IOException {

        if(args.length!=2)
            System.out.println("*** USAGE: report RDFDBNAME INDEXOPTION");
        String rdfdbname = args[0];   //Database name
        int indexoption = Integer.parseInt(args[1]);    //Index option
        String rdfdbpath = "/tmp/" + rdfdbname + "_" + indexoption;

        try{
            //get heap files
            QuadrupleHF = sysdef.JavabaseDB.getQuadrupleHandle();
            EntityHF = sysdef.JavabaseDB.getEntityHandle();
            PredicateHF = sysdef.JavabaseDB.getPredicateHandle();

            System.out.println("Printing statistics of database");
            System.out.println("Database name: " +  rdfdbname + "_" + indexoption);
            System.out.println("Database path: " + rdfdbpath);
            System.out.println("***Record counts***");
            BatchInsert.db_stats();
            System.out.println("***Heap File Records***");
            System.out.println("Quadruple heap file record count: " + QuadrupleHF.getQuadrupleCnt());
            System.out.println("Entity heap file record count: " + EntityHF.getLabelCnt());
            System.out.println("Predicate heap file record count: " + PredicateHF.getLabelCnt());
            System.out.println("***Disk read/write count***");
            System.out.println("Number of disk reads: " + PCounter.rcounter);
            System.out.println("Number of disk writes: " + PCounter.wcounter);

        }catch(Exception e){
            System.out.println("Unable to read database statistics");
        }
    }
}
