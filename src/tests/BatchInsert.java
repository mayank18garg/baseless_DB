package tests;

import global.LID;
import global.QID;
import global.SystemDefs;
import quadrupleheap.Quadruple;
import quadrupleheap.QuadrupleHeapfile;

import java.io.*;

public class BatchInsert {

    static SystemDefs sysdef;

    public static void main(String[] args)
    {

        String dbname = null;   //Database name
        int indexoption = 0;    //Index option
        String inputfile = null; //Datafile from which to load the data
        String rdfdbname = "";

        boolean exists = false;

        if(args.length == 3 )   //Check if the args are DATAFILE DATABASENAME INDEXOPTION
        {
            inputfile = new String(args[0]);
            indexoption = Integer.parseInt(args[1]);
            rdfdbname = new String(args[2]);
            dbname = "/tmp/"+rdfdbname+"_"+indexoption;

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
        File dbfile = new File(dbname);
        if(dbfile.exists()) // check id db exists
        {
            //Database exists. insert records in existing db
            sysdef = new SystemDefs(dbname,0,1000,"Clock",indexoption);
            //existingdb = true;
        }
        else
        {
            //Create new database
            sysdef = new SystemDefs(dbname,10000,1000,"Clock",indexoption);
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

            while((strLine=br.readLine())!=null){

                //split each line to store subject, predicate, object in respective heapfiles
                //ip =  :Joen :knows :Eiwth		0.5232176791516268
                strLine = strLine.replace("\t\t",":").replace(" ","").replace(":"," ").trim();
                String[] input = strLine.split(" ");
                if(input.length!=4) continue;
                String subject = input[0].trim();
                String predicate = input[1].trim();
                String object = input[2].trim();
                String confidence = input[3].trim();

                //insert subject
                try{
                    sub_id = sysdef.JavabaseDB.insertEntity(subject);
                }
                catch(Exception e){
                    System.out.println("Unable to insert subject:+"+subject);
                }

                //insert predicate
                try{
                    pred_id = sysdef.JavabaseDB.insertPredicate(predicate);
                }catch (Exception e){
                    System.out.println("Unable to insert predicate:+"+predicate);
                }

                //insert subject
                try {
                    obj_id = sysdef.JavabaseDB.insertEntity(object);
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

            }

            //parsing done
            System.out.println("Successfully inserted all records");
            System.out.println("Print stats for database");
            db_stats();

        }catch(Exception e){
            System.out.println("Error occured while parsing file");
            e.printStackTrace();
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
