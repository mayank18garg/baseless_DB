package global;

import bufmgr.*;
import diskmgr.*;
import catalog.*;

public class SystemDefs {
  public static BufMgr	JavabaseBM;
  public static rdfDB	JavabaseDB;
  public static Catalog	JavabaseCatalog;
  
  public static String  JavabaseDBName;
  public static String  JavabaseLogName;
  public static boolean MINIBASE_RESTART_FLAG = false;
  public static String	MINIBASE_DBNAME;
  
  public SystemDefs (){};
  
  public SystemDefs(String dbname, int num_pgs, int bufpoolsize,
		    String replacement_policy )
    {
      int logsize;
      
      String real_logname = new String(dbname);
      String real_dbname = new String(dbname);
      
      if (num_pgs == 0) {
	logsize = 500;
      }
      else {
	logsize = 3*num_pgs;
      }
      
      if (replacement_policy == null) {
	replacement_policy = new String("Clock");
      }
      
      init(real_dbname,real_logname, num_pgs, logsize,
	   bufpoolsize, replacement_policy, new Integer(null));
    }

    public SystemDefs(String dbname, int num_pages, int bufpoolsize, String replacement_policy, int indexoption) {

        int logsize;

        String real_logname = new String(dbname);
        String real_dbname = new String(dbname);

        if (num_pages == 0) {
            logsize = 500;
        }
        else {
            logsize = 3*num_pages;
        }

        if (replacement_policy == null) {
            replacement_policy = new String("Clock");
        }

        init(real_dbname,real_logname, num_pages, logsize,
                bufpoolsize, replacement_policy, indexoption);

    }


    public void init( String dbname, String logname,
		    int num_pgs, int maxlogsize,
		    int bufpoolsize, String replacement_policy , int indexoption)
    {
      
      boolean status = true;
      JavabaseBM = null;
      JavabaseDB = null;
      JavabaseDBName = null;
      JavabaseLogName = null;
      JavabaseCatalog = null;
      
      try {
	JavabaseBM = new BufMgr(bufpoolsize, replacement_policy);
	JavabaseDB = new rdfDB(indexoption);
/*
	JavabaseCatalog = new Catalog(); 
*/
      }
      catch (Exception e) {
	System.err.println (""+e);
	e.printStackTrace();
	Runtime.getRuntime().exit(1);
      }
      
      JavabaseDBName = new String(dbname);
      JavabaseLogName = new String(logname);
      MINIBASE_DBNAME = new String(JavabaseDBName);
      
      // create or open the DB
      
      if ((MINIBASE_RESTART_FLAG)||(num_pgs == 0)){//open an existing database
	try {
	  JavabaseDB.openDB(dbname);
	}
	catch (Exception e) {
	  System.err.println (""+e);
	  e.printStackTrace();
	  Runtime.getRuntime().exit(1);
	}
      } 
      else {
	try {
	  JavabaseDB.openDB(dbname, num_pgs);
	  JavabaseBM.flushAllPages();
	}
	catch (Exception e) {
	  System.err.println (""+e);
	  e.printStackTrace();
	  Runtime.getRuntime().exit(1);
	}
      }
    }

    public static void close()
	{
		try
		{
			JavabaseDB.rdfcloseDB();
		}
		catch(Exception e)
		{
			System.out.println ("Closing RDF: ***************");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		try
		{
			JavabaseBM.flushAllPages();
		}
		catch(Exception e)
		{
			System.out.println ("Flushing Pages: ***************");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);

		}
		try
		{

			JavabaseDB.closeDB();
		}
		catch(Exception e)
		{
			System.out.println ("Super DB close: ***************");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);

		}	
	}
}
