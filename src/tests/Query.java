package tests;

import diskmgr.PCounter;
import diskmgr.Stream;
import global.QID;
import global.SystemDefs;
import labelheap.LabelHeapfile;
import quadrupleheap.Quadruple;

import java.io.File;

public class Query {

    static SystemDefs sysdef;
    static LabelHeapfile EntityHF, PredicateHF;
    public static void main(String[] args)
    {

        //query RDFDBNAME INDEXOPTION ORDER SUBJECTFILTER PREDICATEFILTER OBJECTFILTER CONFIDENCEFILTER NUMBUF
        if(args.length!=8){
            System.out.println("***USAGE : query RDFDBNAME INDEXOPTION ORDER SUBJECTFILTER PREDICATEFILTER OBJECTFILTER CONFIDENCEFILTER NUMBUF ***" + args.length);
            return;
        }
        String rdfdbname = args[0];
        int indexoption = Integer.parseInt(args[1]);
        int order = Integer.parseInt(args[2]);
        String subjectFilter = args[3].compareTo("*")==0 ? null : args[3];
        String predicateFilter = args[4].compareTo("*")==0 ? null : args[4];
        String objectFilter = args[5].compareTo("*")==0 ? null :  args[5];
        String confidenceStr = args[6].compareTo("*")==0 ? null : args[6];
        Double confidenceFilter = confidenceStr == null ? 0 : Double.parseDouble(confidenceStr);
        int numbuf = Integer.parseInt(args[7]);

        //db name
        rdfdbname = rdfdbname + "_" +indexoption;

        try{
            File file = new File(rdfdbname);
            if(file.exists()){
                //open existing file
                //initialize buffer pool size as numbuf
                System.out.println("Opening existing db: " + rdfdbname);
                sysdef = new SystemDefs(rdfdbname,0,1000,"Clock",indexoption);
            }else{
                //create new db
                //initialize buffer pool size as numbuf
                System.out.println("Database doesnt exist");
            }
        }catch (Exception e){
            System.out.println("Could not create or open database");
            e.printStackTrace();
        }
        Stream stream = null;
        try{
            stream = sysdef.JavabaseDB.openStream(rdfdbname, order, subjectFilter,
                    predicateFilter, objectFilter, confidenceFilter, numbuf);
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
                //System.out.println(quadruple==null);
                if(quadruple!=null){
                    if(quadruple.getSubjecqid().slotNo != -1) {
                        //quadruple.print();
                        String subject = EntityHF.getLabel(quadruple.getSubjecqid().returnLID()).getLabel();
                        String predicate = PredicateHF.getLabel(quadruple.getPredicateID().returnLID()).getLabel();
                        String object = EntityHF.getLabel(quadruple.getObjecqid().returnLID()).getLabel();
                        Double confidence = quadruple.getConfidence();
                        System.out.println("Quadruple " + i + ": " + subject + " " + predicate + " " + object + " " + confidence);
                        i++;
                    }
                }
            }while(quadruple!=null);

            System.out.println("Query execution complete");
            //get pccounters
            System.out.println("Number of disk reads: " + PCounter.rcounter);
            System.out.println("Number of disk writes: " + PCounter.wcounter);


        }catch (Exception e){
            System.out.println("Could not run query");
            e.printStackTrace();
        } finally {
            stream.closeStream();
            sysdef.close();
        }


    }
}
