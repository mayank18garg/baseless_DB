package tests;

import global.QID;
import global.SystemDefs;
import quadrupleheap.Quadruple;

import java.io.File;
import java.util.stream.Stream;

public class Query {

    static SystemDefs sysdef;

    public static void main(String[] args)
    {

        //query RDFDBNAME INDEXOPTION ORDER SUBJECTFILTER PREDICATEFILTER OBJECTFILTER CONFIDENCEFILTER NUMBUF
        if(args.length!=8){
            System.out.println("***USAGE : query RDFDBNAME INDEXOPTION ORDER SUBJECTFILTER PREDICATEFILTER OBJECTFILTER CONFIDENCEFILTER NUMBUF ***");
        }
        String rdfdbname = args[0];
        int indexoption = Integer.parseInt(args[1]);
        int order = Integer.parseInt(args[2]);
        String subjectFilter = args[3];
        String predicateFilter = args[4];
        String objectFilter = args[5];
        String confidenceFilter = args[6];
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
            QID qid = new QID();
            Quadruple quadruple = null;
            do{
                quadruple = stream.getNext(qid);
                if(quadruple!=null){

                    //String subject = quadruple.getSubjecqid()

                }
            }while(quadruple!=null);


        }catch (Exception e){
            System.out.println("Could not run query");
            e.printStackTrace();
        }


    }
}
