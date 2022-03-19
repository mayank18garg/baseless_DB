package tests;

import diskmgr.PCounter;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import labelheap.LabelHeapfile;
import quadrupleheap.Quadruple;
import quadrupleheap.QuadrupleHeapfile;

import java.io.File;
import java.io.IOException;

public class Report {

    static SystemDefs sysdef;
    static LabelHeapfile EntityHF, PredicateHF;
    static QuadrupleHeapfile QuadrupleHF;

    public static void main(String[] args)
            throws HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, IOException {

        if(args.length!=2) {
            System.out.println("*** USAGE: report RDFDBNAME INDEXOPTION");
            return;
        }

        String rdfdbname = args[0];   //Database name
        int indexoption = Integer.parseInt(args[1]);    //Index option

        //String rdfdbpath = "/Users/jagrutidhondage/Documents/DBMSI/dbfiles/" + rdfdbname + "_" + indexoption; //generate path
        rdfdbname = rdfdbname + "_" + indexoption; //generate db name
        File dbfile;
        try{
            dbfile = new File(rdfdbname);
            if(dbfile.exists()) // check id db exists
            {
                //Database exists. insert records in existing db
                sysdef = new SystemDefs(rdfdbname,0,1000,"Clock",indexoption);
                //existingdb = true;
            }
            else
            {
                System.out.println("Database doesnt exist.");
                return;
            }
        }catch(Exception e){
            System.out.println("Cannot open database");
            e.printStackTrace();
            return;
        }

        try{
            //get heap files
            QuadrupleHF = sysdef.JavabaseDB.getQuadrupleHandle();
            EntityHF = sysdef.JavabaseDB.getEntityHandle();
            PredicateHF = sysdef.JavabaseDB.getPredicateHandle();

            System.out.println("Printing statistics of database");
            System.out.println("Database name: " +  rdfdbname);
            System.out.println("Database path: " + dbfile.getAbsolutePath());
            System.out.println(" DB Size: " + dbfile.length() + " bytes");
            System.out.println(" Page Size: " + sysdef.JavabaseDB.db_page_size() + " bytes");
            System.out.println(" Number of Pages in DB: " + sysdef.JavabaseDB.db_num_pages());
            System.out.println("**********Record counts**********");
            db_stats();
            System.out.println("***Heap File Records***");
            System.out.println("Quadruple heap file record count: " + QuadrupleHF.getQuadrupleCnt());
            System.out.println("Entity heap file record count: " + EntityHF.getLabelCnt());
            System.out.println("Predicate heap file record count: " + PredicateHF.getLabelCnt());
            System.out.println("**********Disk read/write count**********");
            System.out.println("Number of disk reads: " + PCounter.rcounter);
            System.out.println("Number of disk writes: " + PCounter.wcounter);

        }catch(Exception e){
            System.out.println("Unable to read database statistics");
            e.printStackTrace();
        }finally {
            sysdef.close();
        }
    }

    public static void db_stats() {

        System.out.println("Quadruple count: " + sysdef.JavabaseDB.getQuadrupleCnt());
        System.out.println("Entity count:" + sysdef.JavabaseDB.getEntityCnt());
        System.out.println("Predicate count:" +  sysdef.JavabaseDB.getPredicateCnt());
        System.out.println("Subject count:" + sysdef.JavabaseDB.getSubjectCnt());
        System.out.println("Object count:" + sysdef.JavabaseDB.getObjectCnt());

    }
}
