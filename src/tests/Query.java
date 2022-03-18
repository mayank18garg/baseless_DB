package tests;

import diskmgr.PCounter;
import global.QID;
import global.SystemDefs;
import labelheap.LabelHeapfile;
import quadrupleheap.Quadruple;

import java.io.File;
import java.util.stream.Stream;

public class Query {

    static SystemDefs sysdef;
    static LabelHeapfile EntityHF, PredicateHF;
    public static void main(String[] args)
    {

        //query RDFDBNAME INDEXOPTION ORDER SUBJECTFILTER PREDICATEFILTER OBJECTFILTER CONFIDENCEFILTER NUMBUF
        if(args.length!=8){
            System.out.println("***USAGE : query RDFDBNAME INDEXOPTION ORDER SUBJECTFILTER PREDICATEFILTER OBJECTFILTER CONFIDENCEFILTER NUMBUF ***");
        }
        String rdfdbname = args[0];
        int indexoption = Integer.parseInt(args[1]);
        int order = Integer.parseInt(args[2]);
        String subjectFilter = args[3].compareTo("*")==0 ? null : args[3];
        String predicateFilter = args[4].compareTo("*")==0 ? null : args[4];
        String objectFilter = args[5].compareTo("*")==0 ? null :  args[5];
        String confidenceFilter = args[6].compareTo("*")==0 ? null : args[6];
        int numbuf = Integer.parseInt(args[7]);

        //db name
        rdfdbname = "/tmp/"+rdfdbname+"_"+indexoption;

        try{
            File file = new File(rdfdbname);
            if(file.exists()){
                //open existing file
                //initialize buffer pool size as numbuf
                sysdef = new SystemDefs(rdfdbname,0,numbuf,"Clock",indexoption);
            }else{
                //create new db
                //initialize buffer pool size as numbuf
                new SystemDefs(rdfdbname,1000,numbuf,"Clock",indexoption);
            }
        }catch (Exception e){
            System.out.println("Could not create or open database");
            e.printStackTrace();
        }

        try{
            Stream stream = sysdef.JavabaseDB.openStream(rdfdbname, order, subjectFilter,
                    predicateFilter, objectFilter, confidenceFilter);
            if(stream == null)
                System.out.println("Could not open stream");
            //get heap files
            EntityHF = sysdef.JavabaseDB.getEntityHandle();
            PredicateHF = sysdef.JavabaseDB.getPredicateHandle();
            QID qid = new QID();
            Quadruple quadruple = null;
            int i =1;
            do{
                quadruple = stream.getNext(qid);
                if(quadruple!=null){

                    String subject = EntityHF.getLabel(quadruple.getSubjecqid().returnLID());
                    String predicate = PredicateHF.getLabel(quadruple.getPredicateID().returnLID());
                    String object = EntityHF.getLabel(quadruple.getObjecqid().returnLID());
                    Double confidence = quadruple.getConfidence();
                    System.out.println("Quadruple "+ i + ": " + subject + " " + predicate + " " + object + " " + confidence);
                }
            }while(quadruple!=null);

            System.out.println("Query execution complete");
            //get pccounters
            System.out.println("Number of disk reads: " + PCounter.rcounter);
            System.out.println("Number of disk writes: " + PCounter.wcounter);


        }catch (Exception e){
            System.out.println("Could not run query");
            e.printStackTrace();
        }


    }
}
