package tests;

import diskmgr.PCounter;
import global.LID;
import global.QID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import quadrupleheap.*;
import labelheap.*;
import btree.*;
import java.io.*;

public class BatchInsert {

    static SystemDefs sysdef;

    public static void main(String[] args)
    {

        String dbname = null;   //Database name
        int indexoption = 0;    //Index option
        String inputfile = null; //Datafile from which to load the data
        String rdfdbname = "";
        String rdfdbpath = "";
        boolean exists = false;

        if(args.length == 3 )   //Check if the args are DATAFILE DATABASENAME INDEXOPTION
        {
            inputfile = new String(args[0]);
            indexoption = Integer.parseInt(args[1]);
            rdfdbname = new String(args[2]);
            //rdfdbpath = "/Users/jagrutidhondage/Documents/DBMSI/dbfiles/"+rdfdbname+"_"+indexoption;
            rdfdbname = rdfdbname+"_"+indexoption;

            File file = new File(inputfile);
            exists = file.exists();
            //check for input file
            if(!exists)
            {
                System.out.println("*** Input File path:"+inputfile+" dosent exist. ***");
                return;
            }

            if(indexoption>5 || indexoption<0)
            {
                System.out.println("Indexoption range should be between 0-5");
                return;
            }
        }
        else
        {
            System.out.println("*** Usage:BatchInsert DATAFILE INDEXOPTION RDFDBNAME ***");
            return;
        }

        //input file exists. traverse thru the file to insert quadruples
        //get db
        File dbfile = new File(rdfdbname);
        if(dbfile.exists()) // check id db exists
        {
            //Database exists. insert records in existing db
            System.out.println("Opening existing database: "+rdfdbname);
            sysdef = new SystemDefs(rdfdbname,0,1000,"Clock",indexoption);
            //existingdb = true;
        }
        else
        {
            //Create new database
            System.out.println("Creating new database: "+rdfdbname);
            sysdef = new SystemDefs(rdfdbname,10000,1000,"Clock",indexoption);
        }
        //contents of quadruples
        LID sub_id = null, pred_id = null , obj_id = null;
        Quadruple quadruple;
        //quadrupleHeapfile = sysdef.JavabaseDB.getQuadrupleHF();
        try{

            //read input file
            FileReader fr=new FileReader(inputfile);   //reads the file
            BufferedReader br=new BufferedReader(fr);  //creates a buffering character input stream
            String strLine;
            int inputCount =0;
            while((strLine=br.readLine())!=null){

                //split each line to store subject, predicate, object in respective heapfiles
                //ip =  :Joen :knows :Eiwth		0.5232176791516268
                strLine = strLine.replace("\t\t",":").replace(" ","").replace(":"," ").trim();
                String[] input = strLine.split(" ");
                if(input.length!=4){ System.out.println("skipping input " + (++inputCount)); continue;}
                String subject = input[0].trim();
                String predicate = input[1].trim();
                String object = input[2].trim();
                String confidence = input[3].trim();

                //insert subject
                try{

                    sub_id = sysdef.JavabaseDB.insertEntity(subject).returnLID();

                }
                catch(Exception e){
                    System.out.println("Unable to insert subject:+"+subject);
                }

                //insert predicate
                try{
                    pred_id = sysdef.JavabaseDB.insertPredicate(predicate).returnLID();

                }catch (Exception e){
                    System.out.println("Unable to insert predicate:+"+predicate);
                }

                //insert subject
                try {

                    obj_id = sysdef.JavabaseDB.insertEntity(object).returnLID();

                }
                catch(Exception e) {
                    System.out.println("Unable to insert object:+"+object);
                }

                // after inserting in labelheap, create quadruple and insert in quadruple heap file
                quadruple = new Quadruple();
                quadruple.setSubjecqid(sub_id.returnEID());
                quadruple.setPredicateID(pred_id.returnPID());
                quadruple.setobjecqid(obj_id.returnEID());
                quadruple.setConfidence(Double.parseDouble(confidence));

                try{
                    QID qid = sysdef.JavabaseDB.insertQuadruple(quadruple.getQuadrupleByteArray());
                }catch(Exception e){
                    System.out.println("Unable to insert quadruple");
                }
                inputCount++;
            }

            //parsing done
            System.out.println("Successfully parsed " + inputCount +" records");
            System.out.println("Successfully inserted all records");

            //get pccounters
            System.out.println("Number of disk reads: " + PCounter.rcounter);
            System.out.println("Number of disk writes: " + PCounter.wcounter);

            System.out.println("Print stats for database");
            sysdef.JavabaseDB.createIndex(indexoption);
            db_stats();

        }catch(Exception e){
            System.out.println("Error occured while parsing file");
            e.printStackTrace();
        }finally {
            sysdef.close();
        }
    }

    public static void db_stats() throws HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, IOException {
        System.out.println("Quadruple heap file record count: " + sysdef.JavabaseDB.getQuadrupleHandle().getQuadrupleCnt());
        System.out.println("Quadruple count: " + sysdef.JavabaseDB.getQuadrupleCnt());
        System.out.println("Entity count:" + sysdef.JavabaseDB.getEntityCnt());
        System.out.println("Predicate count:" +  sysdef.JavabaseDB.getPredicateCnt());
        System.out.println("Subject count:" + sysdef.JavabaseDB.getSubjectCnt());
        System.out.println("Object count:" + sysdef.JavabaseDB.getObjectCnt());

    }
}
