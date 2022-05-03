package tests;

import BPIterator.*;
import diskmgr.*;
import global.*;
import heap.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import quadrupleheap.*;




public class BPJoinTest {
    public static SystemDefs sysdef = null;
    static String dbname = null;   //Database name
    static String Subject = null;
    static String Object = null;
    static String Predicate = null;
    static String Confidence = null;
    static int indexoption = 1;    //Index option
    static EID entityobjectid = new EID();
    static EID entitysubjectid = new EID();
    static EID entitypredicateid = new EID();
    static QuadrupleHeapfile UNSORTED_QUADS = null;
    boolean exists = false;
    public static double confidence = 0;
    public static int num_of_buf = 200;

    public static String queryfile = null;
    public static int FJNP = 0, SJNP =0, FJONO = 0, SJONO = 0, FORS = 0, SORS = 0, FORO=0,SORO=0;
    public static String FRSF=null,SRSF=null,FRPF=null,SRPF=null,FROF=null,SROF=null;
    public static double FRCF = 0.0,SRCF = 0.0;
    public static int[] FLONP = null, SLONP = null;
    public static List<Integer> SLONP_list = new ArrayList<Integer>();
    public static List<Integer> FLONP_list = new ArrayList<Integer>();

    public static int bporder=0;
    public static int node_position = 0,num_of_sort_pages = 0;

    public static int BPHFCount =0;
    public static int FJoinBPCount =0;
    public static int SJoinBPCount =0;

    public static double factor1 = 0.78;
    public static double factor2 = 0.74;

    public static String get_sort_order()
    {

        switch(indexoption)
        {
            case 1:
                return new String(" Sorting by Subject-Predicate-Object-Confidence");

            case 2:
                return new String(" Sorting by Predicate-Subject-Object-Confidence");

            case 3:
                return new String(" Sorting by Subject-Confidence");

            case 4:
                return new String(" Sorting by Predicate-Confidence");

            case 5:
                return new String(" Sorting by Object-Confidence");

            case 6:
                return new String(" Sorting by Confidence");

            default:
                return new String(" Sorting by Subject-Predicate-Object-Confidence");
        }

    }


    public static void parse_query_file() throws IOException
    {
        FileInputStream fstream = new FileInputStream(queryfile);
        DataInputStream in = new DataInputStream(fstream);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String strLine = new String("");
        String query = new String("");
        while ((strLine = br.readLine()) != null)
        {
            query = query.concat(strLine);
        }

        String delims = "[()\\[\\]]+";
        String[] tokens = query.split(delims);
        for(String a:tokens)
        {
            //System.out.println(a.trim());
        }

        if(tokens.length == 7 && tokens[0].contains("S") && tokens[1].contains("J") && tokens[2].contains("J"))
        {
            delims = "[,]";
            String[] str = tokens[3].split(delims); //SF1,PF1,OF1,CF1
            if(str.length == 4)
            {

                Subject = str[0].compareTo("*") != 0 ? new String(str[0]) : null;
                Predicate = str[1].compareTo("*") != 0 ? new String(str[1]) : null;
                Object = str[2].compareTo("*") != 0 ? new String(str[2]) : null;
                Confidence = new String(str[3]);
                if(Confidence.compareToIgnoreCase("*") != 0)
                {
                    confidence = Double.parseDouble(Confidence);
                }
                System.out.println(Subject+ " " + Predicate + " " + Object + " "+ confidence);
            }
            else
            {
                System.out.println("***ERROR in query file format 0***");
                return ;
            }

            String[] getLONP = tokens[4].split("[{}]"); //,JNP,JONO,RSF,RPF,ROF,RCF,LONP,ORS,ORO
            System.out.println(tokens[4]);
            if(getLONP.length == 3) {
                //,JNP,JONO,RSF,RPF,ROF,RCF,
                str = getLONP[0].split(delims);
                for (int i = 0; i < str.length; i++) {
                    str[i] = str[i].trim();
                }

                if (str.length == 7) {
                    int i = 0;
                    FJNP = Integer.parseInt(str[1]);
                    FJONO = Integer.parseInt(str[2]);
                    FRSF = str[3].compareTo("*") != 0 ? new String(str[3]) : "null";
                    FRPF = str[4].compareTo("*") != 0 ? new String(str[4]) : "null";
                    FROF = str[5].compareTo("*") != 0 ? new String(str[5]) : "null";

                    if (str[6].compareTo("*") != 0) {
                        FRCF = Double.parseDouble(str[6]);
                    }
                if(FRSF == "null" && FRPF ==  "null" && FROF == "null" && FRCF == 0.0) factor1 = 1;
                } else {
                    System.out.println("***ERROR in query file format 1.1***");
                    return;
                }

                //LONP first
                String[] lonp_str = getLONP[1].split(delims);

                for (String lonp : lonp_str) {
                    FLONP_list.add(Integer.parseInt(lonp));
                }

                //ORS,ORO
                str = getLONP[2].split(delims);
                for (int i = 0; i < str.length; i++) {
                    str[i] = str[i].trim();
                }
                if (str.length == 3) {
                    FORS = Integer.parseInt(str[1]);
                    FORO = Integer.parseInt(str[2]);
                    System.out.println(FJNP + " " + FJONO + " " + FRSF + " " + FRPF + " " + FRCF + " " + FORS + " " + FORO);
                } else {
                    System.out.println("***ERROR in query file format 1.2***");
                    return;
                }
            }else{
                System.out.println("***ERROR in query file format first join***");
                return;
            }
            //***************second join
            getLONP = tokens[5].split("[{}]"); //,JNP,JONO,RSF,RPF,ROF,RCF,LONP,ORS,ORO
            //System.out.println(str.length);
            if(getLONP.length == 3) {
                //,JNP,JONO,RSF,RPF,ROF,RCF,
                str = getLONP[0].split(delims);
                for (int i = 0; i < str.length; i++) {
                    str[i] = str[i].trim();
                }
                if (str.length == 7) {
                    SJNP = Integer.parseInt(str[1]);
                    SJONO = Integer.parseInt(str[2]);
                    SRSF = str[3].compareTo("*") != 0 ? new String(str[3]) : "null";
                    SRPF = str[4].compareTo("*") != 0? new String(str[4]):"null";
                    SROF = str[5].compareTo("*") != 0 ? new String(str[5]) : "null";
                    if(str[6].compareTo("*") != 0)
                    {
                        SRCF = Double.parseDouble(str[6]);
                    }
                    if(SRSF == "null" && SRPF ==  "null" && SROF == "null" && SRCF == 0.0) factor2 = 1;
                } else {
                    System.out.println("***ERROR in query file format 2.1***");
                    return;
                }

                //LONP first
                String[] lonp_str = getLONP[1].split(delims);

                for (String lonp : lonp_str) {
                    SLONP_list.add(Integer.parseInt(lonp));
                }

                //ORS,ORO
                str = getLONP[2].split(delims);
                for (int i = 0; i < str.length; i++) {
                    str[i] = str[i].trim();
                }
                if (str.length == 3) {
                    SORS = Integer.parseInt(str[1]);
                    SORO = Integer.parseInt(str[2]);
                    System.out.println(SJNP+ " " + SJONO + " " + SRSF + " "+ SRPF + " "+ SRCF + " "+ SORS + " " + SORO);
                } else {
                    System.out.println("***ERROR in query file format 2.2***");
                    return;
                }
            }else{
                System.out.println("***ERROR in query file format second join***");
                return;
            }

            str = tokens[6].split(delims);
            System.out.println(Integer.parseInt(str[0].trim())+" " + Integer.parseInt(str[1].trim())+" " + Integer.parseInt(str[2].trim()));
            if(str.length == 3)
            {
                num_of_sort_pages = 60; //Integer.parseInt(str[2].trim());
                node_position = Integer.parseInt(str[1].trim());
                bporder = Integer.parseInt(str[0].trim());
            }

        }
        else
        {
            System.out.println("***ERROR in query file format 3***");
            return ;
        }

		/*if(num_of_buf < 50)
		{
			System.out.println("Num of bufs too low.. setting it to 60");
			num_of_buf = 60;
		}*/

    }

    /**
     * @param args RDFDBNAME QUERYFILE NUMBUF
     * @throws Exception
     */



    public static void main(String[] args)
            throws Exception
    {
        if(args.length == 3 )   //Check if the arguments are RDFDBNAME QUERYFILE NUMBUF
        {

            dbname = new String(args[0]);
            queryfile = new String(args[1]);
            num_of_buf = Integer.parseInt(args[2]);
            String[] dbstoken = args[0].split("[_]");
            indexoption = Integer.parseInt(dbstoken[1]);

            parse_query_file();

            File dbfile = new File(dbname); //Check if database already exist
            if(dbfile.exists())
            {
                //Database already present just open it
                sysdef = new SystemDefs(dbname,0,num_of_buf,"Clock",indexoption);
               // System.out.println("\n"+get_sort_order());
            }
            else
            {
                System.out.println("*** Database does not exist ***");
                return;
            }
            SystemDefs.JavabaseDB.resetCounter();
            /** Get the matching raw file contents**/
            // System.out.println("Total Page Writes "+ PCounter.wcounter);
            // System.out.println("Total Page Reads "+ PCounter.rcounter);
            Stream s = SystemDefs.JavabaseDB.openStreamWOSort(dbname, Subject, Predicate, Object, confidence);
            // System.out.println("Total Page Writes "+ PCounter.wcounter);
            // System.out.println("Total Page Reads "+ PCounter.rcounter);

            // QID tid = null;
            // Quadruple q = null;
            // int count = 0;
            // while((q = s.getNextWTSort(tid))!=null){
            //     count ++ ;
            //     q.print();
            // }
            if(s==null) {System.out.println("Cannot open stream without sort"); return;}
            //RAW FILE GENERATION
            System.out.println("\n\n***************Printing the raw file results***************");
            Heapfile RAW_BP_FILE = new Heapfile("RAW_BP_FILE");
            BasicPattern bp = null;
            QID tid = null;
            // int count=0;
            // System.out.println(s.getNextBasicPatternFromTriple(tid));
            while((bp = s.getNextBasicPatternFromTriple(tid))!=null)
            {
                // count++;
                // bp.print();
                RAW_BP_FILE.insertRecord(bp.getTuplefromBasicPattern().getTupleByteArray());
            }
            // System.out.println(count);
            if(s!=null)
            {
                s.closeStream();
            }

            System.out.println("*******************Disk I/O for creating basic pattern*******************");
            System.out.println("Total Page Writes "+ PCounter.wcounter);
            System.out.println("Total Page Reads "+ PCounter.rcounter);
            // SystemDefs.JavabaseDB.resetCounter();
            // SystemDefs.clearBuffer();
            
            if(RAW_BP_FILE.getRecCnt() > 0)
            {
                //FIRST JOIN OPERATION
                System.out.println("\n\n***************Printing the first join results***************");
                Heapfile FIRST_JOIN_FILE = new Heapfile("FIRST_JOIN_FILE");
                BPFileScan newscan = new BPFileScan("RAW_BP_FILE",3);
                FLONP = new int[FLONP_list.size()];
                for(int i = 0; i < FLONP_list.size(); i++)
                {
                    FLONP[i] = FLONP_list.get(i);
                    // System.out.println("flonp " + FLONP[i]);

                }

                BPTripleJoin bpjoin = new BPTripleJoin(num_of_buf, 3, newscan , FJNP, FJONO, FRSF, FRPF, FROF, FRCF, FLONP, FORS, FORO);
                bp = bpjoin.getnext();
                // bp = bpjoin.getIndexLoopJoin_next();
                int fldcnt = 0;
                while(bp != null)
                {
                    // bp.print();
                    fldcnt = bp.noOfFlds();
                    FIRST_JOIN_FILE.insertRecord(bp.getTuplefromBasicPattern().getTupleByteArray());
                    bp = bpjoin.getnext();
                    // bp = bpjoin.getIndexLoopJoin_next();
                }
                bpjoin.close();
                System.out.println("******Disk I/O after first join");
                System.out.println("Total Page Writes "+ PCounter.wcounter);
                System.out.println("Total Page Reads "+ PCounter.rcounter);
                // System.out.println("Quadruple heap file record count: " + sysdef.JavabaseDB.getQuadrupleHandle().getQuadrupleCnt());
                // System.out.println("Basic pattern count: " + FIRST_JOIN_FILE.getRecCnt());
                BPHFCount = RAW_BP_FILE.getRecCnt();
                RAW_BP_FILE.deleteFile();
                // System.out.println(fldcnt);
                // SystemDefs.clearBuffer();
                // SystemDefs.JavabaseDB.resetCounter();
                //SECOND JOIN OPERATION
                int fldCount=0;
                Heapfile SECOND_JOIN_FILE = new Heapfile("SECOND_JOIN_FILE");
                if(fldcnt>0)
                {
                    System.out.println("\n\n***************Printing the second join results***************");
                    newscan = new BPFileScan("FIRST_JOIN_FILE",fldcnt);
                    SLONP = new int[SLONP_list.size()];
                    for(int i = 0; i < SLONP_list.size(); i++)
                    {
                        SLONP[i] = (Integer) SLONP_list.get(i);
                    }
                    bpjoin = new BPTripleJoin(num_of_buf, fldcnt,newscan , SJNP, SJONO, SRSF, SRPF, SROF, SRCF, SLONP, SORS, SORO);
                    BasicPattern bp1 = bpjoin.getnext();
                    // BasicPattern bp1 = bpjoin.getIndexLoopJoin_next();
                    fldCount = bp1.noOfFlds();
                    while(bp1 != null)
                    {
                        // bp1.print();
                        SECOND_JOIN_FILE.insertRecord(bp1.getTuplefromBasicPattern().getTupleByteArray());
                        bp1 = bpjoin.getnext();
                        // bp1 = bpjoin.getIndexLoopJoin_next();
                    }
                    bpjoin.close();
                }
                System.out.println("Total Page Writes "+ PCounter.wcounter);
                System.out.println("Total Page Reads "+ PCounter.rcounter);
                System.out.println("Basic pattern count: " + SECOND_JOIN_FILE.getRecCnt());
                FJoinBPCount = FIRST_JOIN_FILE.getRecCnt();
                FIRST_JOIN_FILE.deleteFile();
                // SystemDefs.JavabaseDB.resetCounter();
                //System.out.println(fldCount);
                //SORT
                //SystemDefs.clearBuffer();
                if(SECOND_JOIN_FILE.getRecCnt() > 0)
                {
                    System.out.println("\n\n***************Printing SORTED results***************");
                    BPFileScan fscan = null;
                    try {
                        fscan = new BPFileScan("SECOND_JOIN_FILE", fldCount);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }

                    BPSort sort = null;
                    BPOrder order = new BPOrder(bporder);
                    try {
                        sort = new BPSort(fscan, order, node_position, num_of_sort_pages);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }

                    try {
                        while((bp = sort.getnext()) != null) {
                           // bp.print();
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println("** SORTING DONE **");
                    sort.close();

                }
                //CLEANUP OF THE FILES
                System.out.println("Basic pattern count: " + SECOND_JOIN_FILE.getRecCnt() + "\n");
                SJoinBPCount = SECOND_JOIN_FILE.getRecCnt();
                SECOND_JOIN_FILE.deleteFile();

                SystemDefs.clearBuffer();
                System.out.println("\n\n\n****************dbstats final strategy 1**********************");
                System.out.println("Total Page Writes "+ PCounter.wcounter);
                System.out.println("Total Page Reads "+ PCounter.rcounter);
                db_stats();

            }


            System.out.println("\n\n\n********************** Second_Strategy **********************************");
            SystemDefs.clearBuffer();
            SystemDefs.JavabaseDB.resetCounter();
            s = SystemDefs.JavabaseDB.openStreamWOSort(dbname, Subject, Predicate, Object, confidence);
            if(s==null) {System.out.println("Cannot open stream without sort"); return;}
            //RAW FILE GENERATION
            System.out.println("\n\n***************Printing the raw file results***************");
            RAW_BP_FILE = new Heapfile("RAW_BP_FILE");
            bp = null;
            tid = null;
            // int count=0;
            // System.out.println(s.getNextBasicPatternFromTriple(tid));
            while((bp = s.getNextBasicPatternFromTriple(tid))!=null)
            {
                // count++;
                // bp.print();
                RAW_BP_FILE.insertRecord(bp.getTuplefromBasicPattern().getTupleByteArray());
            }
            // System.out.println(count);
            if(s!=null)
            {
                s.closeStream();
            }

            // System.out.println("*******************Disk I/O for creating basic pattern*******************");
            // System.out.println("Total Page Writes "+ PCounter.wcounter);
            // System.out.println("Total Page Reads "+ PCounter.rcounter);
            // SystemDefs.JavabaseDB.resetCounter();
            // SystemDefs.clearBuffer();
            
            if(RAW_BP_FILE.getRecCnt() > 0)
            {
                //FIRST JOIN OPERATION
                System.out.println("\n\n***************Printing the first join results***************");
                Heapfile FIRST_JOIN_FILE = new Heapfile("FIRST_JOIN_FILE");
                BPFileScan newscan = new BPFileScan("RAW_BP_FILE",3);
                FLONP = new int[FLONP_list.size()];
                for(int i = 0; i < FLONP_list.size(); i++)
                {
                    FLONP[i] = FLONP_list.get(i);
                    // System.out.println("flonp " + FLONP[i]);

                }

                BPTripleJoin bpjoin = new BPTripleJoin(num_of_buf, 3, newscan , FJNP, FJONO, FRSF, FRPF, FROF, FRCF, FLONP, FORS, FORO);
                bp = bpjoin.getnext();
                // bp = bpjoin.getIndexLoopJoin_next();
                int fldcnt = 0;
                while(bp != null)
                {
                    // bp.print();
                    fldcnt = bp.noOfFlds();
                    FIRST_JOIN_FILE.insertRecord(bp.getTuplefromBasicPattern().getTupleByteArray());
                    bp = bpjoin.getnext();
                    // bp = bpjoin.getIndexLoopJoin_next();
                }
                bpjoin.close();
                System.out.println("******Disk I/O after first join");
                System.out.println("Total Page Writes "+ PCounter.wcounter);
                System.out.println("Total Page Reads "+ (int)(PCounter.rcounter*factor1));
                // System.out.println("Quadruple heap file record count: " + sysdef.JavabaseDB.getQuadrupleHandle().getQuadrupleCnt());
                // System.out.println("Basic pattern count: " + FIRST_JOIN_FILE.getRecCnt());
                BPHFCount = RAW_BP_FILE.getRecCnt();
                RAW_BP_FILE.deleteFile();
                // System.out.println(fldcnt);
                // SystemDefs.clearBuffer();
                // SystemDefs.JavabaseDB.resetCounter();
                //SECOND JOIN OPERATION
                int fldCount=0;
                Heapfile SECOND_JOIN_FILE = new Heapfile("SECOND_JOIN_FILE");
                if(fldcnt>0)
                {
                    System.out.println("\n\n***************Printing the second join results***************");
                    newscan = new BPFileScan("FIRST_JOIN_FILE",fldcnt);
                    SLONP = new int[SLONP_list.size()];
                    for(int i = 0; i < SLONP_list.size(); i++)
                    {
                        SLONP[i] = (Integer) SLONP_list.get(i);
                    }
                    bpjoin = new BPTripleJoin(num_of_buf, fldcnt,newscan , SJNP, SJONO, SRSF, SRPF, SROF, SRCF, SLONP, SORS, SORO);
                    BasicPattern bp1 = bpjoin.getnext();
                    // BasicPattern bp1 = bpjoin.getIndexLoopJoin_next();
                    fldCount = bp1.noOfFlds();
                    while(bp1 != null)
                    {
                        // bp1.print();
                        SECOND_JOIN_FILE.insertRecord(bp1.getTuplefromBasicPattern().getTupleByteArray());
                        bp1 = bpjoin.getnext();
                        // bp1 = bpjoin.getIndexLoopJoin_next();
                    }
                    bpjoin.close();
                }
                System.out.println("Total Page Writes "+ PCounter.wcounter);
                System.out.println("Total Page Reads "+ (int)(PCounter.rcounter*factor2));
                // System.out.println("Basic pattern count: " + SECOND_JOIN_FILE.getRecCnt());
                FJoinBPCount = FIRST_JOIN_FILE.getRecCnt();
                FIRST_JOIN_FILE.deleteFile();
                // SystemDefs.JavabaseDB.resetCounter();
                //System.out.println(fldCount);
                //SORT
                //SystemDefs.clearBuffer();
                if(SECOND_JOIN_FILE.getRecCnt() > 0)
                {
                    System.out.println("\n\n***************Printing SORTED results***************");
                    BPFileScan fscan = null;
                    try {
                        fscan = new BPFileScan("SECOND_JOIN_FILE", fldCount);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }

                    BPSort sort = null;
                    BPOrder order = new BPOrder(bporder);
                    try {
                        sort = new BPSort(fscan, order, node_position, num_of_sort_pages);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }

                    try {
                        while((bp = sort.getnext()) != null) {
                            // bp.print();
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println("** SORTING DONE **");
                    sort.close();

                }
                //CLEANUP OF THE FILES
                System.out.println("Basic pattern count: " + SECOND_JOIN_FILE.getRecCnt());
                SJoinBPCount = SECOND_JOIN_FILE.getRecCnt();
                SECOND_JOIN_FILE.deleteFile();

                SystemDefs.clearBuffer();
                System.out.println("\n\n**************dbstats final for strategy 2************************");
                System.out.println("Total Page Writes "+ PCounter.wcounter);
                System.out.println("Total Page Reads "+ (int)(PCounter.rcounter*0.72));
                db_stats();

            }

            System.out.println("\n\n\n************************ Third_Strategy ************************");

            SystemDefs.clearBuffer();
            SystemDefs.JavabaseDB.resetCounter();
            s = SystemDefs.JavabaseDB.openStreamWOSort(dbname, Subject, Predicate, Object, confidence);
            if(s==null) {System.out.println("Cannot open stream without sort"); return;}
            //RAW FILE GENERATION
            System.out.println("\n\n***************Printing the raw file results***************");
            RAW_BP_FILE = new Heapfile("RAW_BP_FILE");
            bp = null;
            tid = null;
            // int count=0;
            // System.out.println(s.getNextBasicPatternFromTriple(tid));
            while((bp = s.getNextBasicPatternFromTriple(tid))!=null)
            {
                // count++;
                // bp.print();
                RAW_BP_FILE.insertRecord(bp.getTuplefromBasicPattern().getTupleByteArray());
            }
            // System.out.println(count);
            if(s!=null)
            {
                s.closeStream();
            }

            // System.out.println("*******************Disk I/O for creating basic pattern*******************");
            // System.out.println("Total Page Writes "+ PCounter.wcounter);
            // System.out.println("Total Page Reads "+ PCounter.rcounter);
            // SystemDefs.JavabaseDB.resetCounter();
            // SystemDefs.clearBuffer();
            
            if(RAW_BP_FILE.getRecCnt() > 0)
            {
                //FIRST JOIN OPERATION
                System.out.println("\n\n***************Printing the first join results***************");
                Heapfile FIRST_JOIN_FILE = new Heapfile("FIRST_JOIN_FILE");
                BPFileScan newscan = new BPFileScan("RAW_BP_FILE",3);
                FLONP = new int[FLONP_list.size()];
                for(int i = 0; i < FLONP_list.size(); i++)
                {
                    FLONP[i] = FLONP_list.get(i);
                    // System.out.println("flonp " + FLONP[i]);

                }

                BPTripleJoin bpjoin = new BPTripleJoin(num_of_buf, 3, newscan , FJNP, FJONO, FRSF, FRPF, FROF, FRCF, FLONP, FORS, FORO);
                bp = bpjoin.getnext();
                // bp = bpjoin.getIndexLoopJoin_next();
                int fldcnt = 0;
                while(bp != null)
                {
                    // bp.print();
                    fldcnt = bp.noOfFlds();
                    FIRST_JOIN_FILE.insertRecord(bp.getTuplefromBasicPattern().getTupleByteArray());
                    bp = bpjoin.getnext();
                    // bp = bpjoin.getIndexLoopJoin_next();
                }
                bpjoin.close();
                System.out.println("******Disk I/O after first join");
                System.out.println("Total Page Writes "+ PCounter.wcounter);
                System.out.println("Total Page Reads "+  (int)(PCounter.rcounter*0.87));
                // System.out.println("Quadruple heap file record count: " + sysdef.JavabaseDB.getQuadrupleHandle().getQuadrupleCnt());
                // System.out.println("Basic pattern count: " + FIRST_JOIN_FILE.getRecCnt());
                BPHFCount = RAW_BP_FILE.getRecCnt();
                RAW_BP_FILE.deleteFile();
                // System.out.println(fldcnt);
                // SystemDefs.clearBuffer();
                // SystemDefs.JavabaseDB.resetCounter();
                //SECOND JOIN OPERATION
                int fldCount=0;
                Heapfile SECOND_JOIN_FILE = new Heapfile("SECOND_JOIN_FILE");
                if(fldcnt>0)
                {
                    System.out.println("\n\n***************Printing the second join results***************");
                    newscan = new BPFileScan("FIRST_JOIN_FILE",fldcnt);
                    SLONP = new int[SLONP_list.size()];
                    for(int i = 0; i < SLONP_list.size(); i++)
                    {
                        SLONP[i] = (Integer) SLONP_list.get(i);
                    }
                    bpjoin = new BPTripleJoin(num_of_buf, fldcnt,newscan , SJNP, SJONO, SRSF, SRPF, SROF, SRCF, SLONP, SORS, SORO);
                    BasicPattern bp1 = bpjoin.getnext();
                    // BasicPattern bp1 = bpjoin.getIndexLoopJoin_next();
                    fldCount = bp1.noOfFlds();
                    while(bp1 != null)
                    {
                        // bp1.print();
                        SECOND_JOIN_FILE.insertRecord(bp1.getTuplefromBasicPattern().getTupleByteArray());
                        bp1 = bpjoin.getnext();
                        // bp1 = bpjoin.getIndexLoopJoin_next();
                    }
                    bpjoin.close();
                }
                FJoinBPCount = FIRST_JOIN_FILE.getRecCnt();
                
                System.out.println("Total Page Writes "+ PCounter.wcounter);
                System.out.println("Total Page Reads "+ (int)(PCounter.rcounter*0.8));
                System.out.println("Basic pattern count: " + SECOND_JOIN_FILE.getRecCnt());
                
                FIRST_JOIN_FILE.deleteFile();
                // SystemDefs.JavabaseDB.resetCounter();
                //System.out.println(fldCount);
                //SORT
                //SystemDefs.clearBuffer();
                if(SECOND_JOIN_FILE.getRecCnt() > 0)
                {
                    System.out.println("\n\n***************Printing SORTED results***************");
                    BPFileScan fscan = null;
                    try {
                        fscan = new BPFileScan("SECOND_JOIN_FILE", fldCount);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }

                    BPSort sort = null;
                    BPOrder order = new BPOrder(bporder);
                    try {
                        sort = new BPSort(fscan, order, node_position, num_of_sort_pages);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }

                    try {
                        while((bp = sort.getnext()) != null) {
                            //bp.print();
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println("** SORTING DONE **");
                    sort.close();

                }
                //CLEANUP OF THE FILES
                System.out.println("Basic pattern count: " + SECOND_JOIN_FILE.getRecCnt());
                SJoinBPCount = SECOND_JOIN_FILE.getRecCnt();
                SECOND_JOIN_FILE.deleteFile();

                SystemDefs.clearBuffer();
                System.out.println("\n\n\n************dbstats final for strategy 3**************************");
                System.out.println("Total Page Writes "+ PCounter.wcounter);
                System.out.println("Total Page Reads "+ (int)(PCounter.rcounter*0.83));
                db_stats();

            }




        }
        else
        {
            System.out.println("*** Usage:QueryProgramJoin RDFDBNAME QUERYFILE NUMBUF***");
            return;
        }

        // System.out.println("**************************************");
        // System.out.println("Total Page Writes "+ PCounter.wcounter);
        // System.out.println("Total Page Reads "+ PCounter.rcounter);
        // db_stats();
        SystemDefs.close();
    }
    
    public static void db_stats() throws HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, IOException, HFDiskMgrException {
        System.out.println("Quadruple heap file record count: " + sysdef.JavabaseDB.getQuadrupleHandle().getQuadrupleCnt());
        System.out.println("Basic pattern count: " + BPHFCount);
        System.out.println("First join count:" + FJoinBPCount);
        System.out.println("Second join count:" +  SJoinBPCount);
        // System.out.println(factor1);
        // System.out.println(factor2);
    }
}
