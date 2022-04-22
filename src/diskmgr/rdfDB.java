/* File DB.java */

package diskmgr;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeSet;

import bufmgr.*;
import global.*;
import labelheap.*;
import quadrupleheap.*;
import btree.*;

public class rdfDB extends DB implements GlobalConst {
  private QuadrupleHeapfile TEMPQuadHF;		//TEMPORARY HEAP FILE FOR SORTING
    
	private QuadrupleHeapfile QuadrupleHeap; 	  		//Quadruples Heap file to store Quadruples
	private LabelHeapfile EntityHeap; 	  		//Entity Heap file to store subjects/objects
	private LabelHeapfile PredicateHeap;   		//Predicates Heap file to store predicates

	private LabelHeapBTreeFile EntityBT;  		//BTree Index file on Entity Heap file
	private LabelHeapBTreeFile PredicateBT; 	//BTree Predicate file on Predicate Heap file
	private QuadrupleBTreeFile QuadrupleBT; 		//BTree on all fields

	private int SubjectsCnt = 0; 			//Total count of subjects in RDF
	private int ObjectsCnt = 0; 				//Total count of objects in RDF
	private int PredicatesCnt = 0; 			//Total count of predicates in RDF
	private int QuadruplesCnt = 0; 				//Total count of Quadruple in RDF
	private int EntitiesCnt = 0;         	//Total count of entities in RDF

	private String dbname; 				//RDF Database name
	private int type; //indexoption

  private QuadrupleBTreeFile QuadrupleBTIndex; 	//BTree file for the index options given
	// INDEX OPTIONS	
	//(1) BTree Index file on confidence
	//(2) BTree Index file on subject and confidence
	//(3) BTree Index file on object and confidence
	//(4) BTree Index file on predicate and confidence
	//(5) BTree Index file on subject

public LabelHeapBTreeFile getPredicateBtree() throws GetFileEntryException, PinPageException, ConstructPageException{
	PredicateBT = new LabelHeapBTreeFile(dbname+"/predicateBT");
		// return _BTree;
	return PredicateBT;
}

public LabelHeapBTreeFile getEntityBtree() throws GetFileEntryException, PinPageException, ConstructPageException{
	EntityBT = new LabelHeapBTreeFile(dbname+"/entityBT");
		// return _BTree;
	return EntityBT;
}
  public QuadrupleHeapfile getQuadrupleHandle() {
		// TODO Auto-generated method stub
		return QuadrupleHeap;
	}

	public LabelHeapfile getEntityHandle() {
		// TODO Auto-generated method stub
		return EntityHeap;
	}
	public LabelHeapfile getPredicateHandle() {
		// TODO Auto-generated method stub
		return PredicateHeap;
	}
	
	public QuadrupleHeapfile getTEMP_Quadruple_HF() {
		return TEMPQuadHF;
	}

	public QuadrupleBTreeFile getQuadrupleBTIndex()
			throws GetFileEntryException, PinPageException, ConstructPageException, IOException {
		QuadrupleBTIndex = new QuadrupleBTreeFile(dbname+"/Quadruple_BTreeIndex");
		return QuadrupleBTIndex;
	}

  public QuadrupleBTreeFile getQuadrupleBT()
		  throws GetFileEntryException, PinPageException, ConstructPageException, IOException {
		QuadrupleBT = new QuadrupleBTreeFile(dbname+"/quadrupleBT");
		return QuadrupleBT;
	}

	public void rdfcloseDB() 
	throws 	PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException
	{
		try {

			if(EntityBT != null)
			{
				EntityBT.close();

			}
			if(PredicateBT != null)
			{
				PredicateBT.close();

			}
			if(QuadrupleBT != null)
			{
				QuadrupleBT.close();

			}
			if(QuadrupleBTIndex != null)
			{
				QuadrupleBTIndex.close();

			}
			if(TEMPQuadHF != null && TEMPQuadHF != getQuadrupleHandle())
			{
				TEMPQuadHF.deleteFile();

			}
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Stream openStream(String dbname,int orderType, String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter,
							 int numbuf)
	{
		Stream streamObj = null;
		try {
			streamObj = new Stream( dbname, orderType, subjectFilter,  predicateFilter, objectFilter, confidenceFilter, numbuf);
		}
		catch(Exception e){
			e.printStackTrace();
		}

		return streamObj;

	}

	public Stream openStreamWOSort(String dbname, String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter)
	{
		Stream streamObj = null;
		try {
			streamObj = new Stream( dbname, subjectFilter,  predicateFilter, objectFilter, confidenceFilter);
		}
		catch(Exception e){
			e.printStackTrace();
		}

		return streamObj;

	}
  
  /**Constructor for the RDF database. 
	 * @param indexOption is an integer denoting the different clus-tering and indexing strategies you will use for the rdf database.
	 * @Note: Each RDF database contains:
	 * one QuadrupleHeapFile to store the Quadruples,
	 * one LabelHeapfile to store entity labels, 
	 * and another LabelHeapfile to store subject labels.
	 * You can create as many btree index files as you want over these Quadruple and label heap files
	 */
  	public rdfDB(int indexOption){
		  type = indexOption;
	}
	public void resetCounter(){
		  System.out.println("Reseting PCCounter");
		  PCounter.reset();
	}
	public void initFiles()
	{
		int keytype = AttrType.attrString;

		/** Initialize counter to zero **/ 
		PCounter.initialize();
		
		//Create TEMP Quadruple heap file
		try
		{ 
			TEMPQuadHF = new QuadrupleHeapfile("tempresult");
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
				
		//Create Quadruple heap file
		try
		{ 
			//System.out.println("Creating new Quadruple heapfile");
			QuadrupleHeap = new QuadrupleHeapfile(dbname+"/quadrupleHF");

		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		//Create Entity heap file: (Entity:Subject/Object)
		try
		{
			//System.out.println("Creating new entity heapfile");
			EntityHeap = new LabelHeapfile(dbname+"/entityHF");
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		//Create Predicate heap file: (Predicates)
		try
		{
			//System.out.println("Creating new predicate heapfile");
			PredicateHeap = new LabelHeapfile(dbname+"/predicateHF");
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}


		//Create Entity Binary tree file
		try
		{
			//System.out.println("Creating new entity Binary Tree file");
			EntityBT = new LabelHeapBTreeFile(dbname+"/entityBT",keytype,255,1);
			EntityBT.close();
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		//Create Predicate Binary tree file
		try
		{
			//System.out.println("Creating new Predicate Binary Tree file");
			PredicateBT = new LabelHeapBTreeFile(dbname+"/predicateBT",keytype,255,1);
			PredicateBT.close();
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
				
		//Create Quadruple Binary tree file
		try
		{
			//System.out.println("Creating new Quadruple Binary Tree file");
			QuadrupleBT = new QuadrupleBTreeFile(dbname+"/quadrupleBT",keytype,255,1);
			QuadrupleBT.close();
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		//Now create btree index files as per the index option
		try
		{
			QuadrupleBTIndex = new QuadrupleBTreeFile(dbname+"/Quadruple_BTreeIndex",keytype,255,1);
			QuadrupleBTIndex.close();
			// createIndex(type);
		}
		catch(Exception e)
		{
			System.err.println ("Error: B tree index cannot be created"+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		System.out.println("successfully initiated All heap files and Btrees ");
	}

  /**
	 *  Get count of Quadruple in RDF DB
	 *  @return int number of Quadruple
	 */ 
	public int getQuadrupleCnt()
	{	
		try
		{
			QuadrupleHeap = new QuadrupleHeapfile(dbname+"/quadrupleHF");
			QuadruplesCnt = QuadrupleHeap.getQuadrupleCnt();
		}
		catch (Exception e) 
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		return QuadruplesCnt;
	}

  /**
     *  Get count of Entities(unique) in RDF DB
     *  @return int number of distinct Entities
     */ 
    public int getEntityCnt()
    {
            try
            {
                    EntityHeap = new LabelHeapfile(dbname+"/entityHF");
                    EntitiesCnt = EntityHeap.getLabelCnt();
            }
            catch (Exception e) 
            {
                    System.err.println (""+e);
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
            }
            return EntitiesCnt;
    }

    /**
	 *  Get count of Predicates(unique) in RDF DB
	 *  @return int number of distinct Predicates
	 */ 
	public int getPredicateCnt()
	{
		try
		{
			PredicateHeap = new LabelHeapfile(dbname+"/predicateHF");
			PredicatesCnt = PredicateHeap.getLabelCnt();
		}
		catch (Exception e) 
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		return PredicatesCnt;
	}

  public int getSubjectCnt()
  {
      SubjectsCnt = 0;
      KeyDataEntry entry = null;
      TreeSet<EID> subjectSet = new TreeSet<EID>((s1, s2) -> {
          if(s1.equals(s2)) return 0;
		  if(s1.pageNo.pid == s2.pageNo.pid) return s1.slotNo - s2.slotNo;
		  else return s1.pageNo.pid - s2.pageNo.pid; // to avoid skew tree
      });
      try
      {
          QuadrupleBT = new QuadrupleBTreeFile(dbname+"/quadrupleBT");
          QuadrupleBTFileScan scan = QuadrupleBT.new_scan(null,null);
          do{
              entry = scan.get_next();
              if(entry!=null){
                  QID qid =  ((QuadrupleLeafData)entry.data).getData();
                  Quadruple quad = QuadrupleHeap.getQuadruple(qid);
                  EID sub_id = quad.getSubjecqid();
        
                  subjectSet.add(sub_id);
              }
          }while(entry!=null);
          SubjectsCnt = subjectSet.size();

          scan.DestroyBTreeFileScan();
          QuadrupleBT.close();

      }
      catch(Exception e)
      {
          System.err.println (""+e);
          e.printStackTrace();
      }
      return SubjectsCnt;
    }

    public int getObjectCnt()
    {
        ObjectsCnt = 0;
        KeyDataEntry entry = null;
        TreeSet<EID> objectSet = new TreeSet<>((o1, o2) -> {
			if (o1.equals(o2)) return 0;
			// to avoid skew tree
			if(o1.pageNo.pid == o2.pageNo.pid) return o1.slotNo - o2.slotNo;
			else return o1.pageNo.pid - o2.pageNo.pid;
		});
        try
        {
            QuadrupleBT = new QuadrupleBTreeFile(dbname+"/quadrupleBT");
            QuadrupleBTFileScan scan = QuadrupleBT.new_scan(null,null);
            do{
                entry = scan.get_next();
                if(entry!=null){
                    QID qid =  ((QuadrupleLeafData)entry.data).getData();
                    Quadruple quad = QuadrupleHeap.getQuadruple(qid);
                    EID obj_id = quad.getObjecqid();
					objectSet.add(obj_id);
                }
            }while(entry!=null);
            ObjectsCnt = objectSet.size();
            scan.DestroyBTreeFileScan();
            QuadrupleBT.close();

        }
        catch(Exception e)
        {
            System.err.println (""+e);
            e.printStackTrace();
        }
        return ObjectsCnt;
    }

	/**
	 * Insert a entity into the EntityHeapFIle
	 * @param EntityLabel String representing Subject/Object
	 */
	public EID insertEntity(String EntityLabel) 
	{
	    int KeyType = AttrType.attrString;
        KeyClass key = new StringKey(EntityLabel);
        EID entityid = null;

        //Open ENTITY BTree Index file
        try
        {
			EntityBT = new LabelHeapBTreeFile(dbname+"/entityBT");


			LID lid = null;
			KeyClass low_key = new StringKey(EntityLabel);
			KeyClass high_key = new StringKey(EntityLabel);
			KeyDataEntry entry = null;

			//Start Scaning Btree to check if entity already present
			LabelHeapBTFileScan scan = EntityBT.new_scan(low_key,high_key);
			entry = scan.get_next();
			if(entry!=null)
			{
				if(EntityLabel.equals(((StringKey)(entry.key)).getKey()))
				{
						//return already existing EID ( convert lid to EID)
						lid =  ((LabelHeapLeafData)entry.data).getData();
						entityid = lid.returnEID();
						scan.DestroyBTreeFileScan();
						EntityBT.close();
						return entityid;
				}
			}

			scan.DestroyBTreeFileScan();
			//Insert into Entity HeapFile
			lid = EntityHeap.insertLabel(EntityLabel);

			//Insert into Entity Btree file key,lid
			EntityBT.insert(key,lid);

			entityid = lid.returnEID();
			EntityBT.close();
        }
        catch(Exception e)
        {
                System.err.println ("*** Error:entity cannot be inserted");
                e.printStackTrace();
        }

        return entityid; //Return EID
	}


	/**
	 * Delete a entity into the EntityHeapFile
	 * @param EntityLabel String representing Subject/Object
	 * @return boolean success when deleted else false
	 */
	public boolean deleteEntity(String EntityLabel)
	{
        boolean success = false;
        int KeyType = AttrType.attrString;
        KeyClass key = new StringKey(EntityLabel);
        EID entityid = null;

        //Open ENTITY BTree Index file
        try
        {
			EntityHeap = new LabelHeapfile(dbname+"/entityHF");
			EntityBT = new LabelHeapBTreeFile(dbname+"/entityBT");

			LID lid = null;
			KeyClass low_key = new StringKey(EntityLabel);
			KeyClass high_key = new StringKey(EntityLabel);
			KeyDataEntry entry = null;

			//Start Scaning Btree to check if entity already present
			LabelHeapBTFileScan scan = EntityBT.new_scan(low_key,high_key);
			entry = scan.get_next();
			if(entry!=null)
			{
				if(EntityLabel.equals(((StringKey)(entry.key)).getKey()))
				{
						lid =  ((LabelHeapLeafData)entry.data).getData();
						success = EntityHeap.deleteLabel(lid) & EntityBT.Delete(low_key,lid);
				}
			}
			scan.DestroyBTreeFileScan();
			EntityBT.close();
        }
        catch(Exception e)
        {
                System.err.println ("*** Error: entity cannot be deleted " + e);
                e.printStackTrace();
        }
        return success;
	}

	public PID insertPredicate(String PredicateLabel)
	{
        PID predicateid = null;
        LID lid = null;

        int KeyType = AttrType.attrString;
        KeyClass key = new StringKey(PredicateLabel);

        //Open PREDICATE BTree Index file
        try
        {
                PredicateBT = new LabelHeapBTreeFile(dbname+"/predicateBT");
                KeyClass low_key = new StringKey(PredicateLabel);
                KeyClass high_key = new StringKey(PredicateLabel);
                KeyDataEntry entry = null;

                //Start Scaning Btree to check if  predicate already present
                LabelHeapBTFileScan scan = PredicateBT.new_scan(low_key,high_key);
                entry = scan.get_next();
                if(entry != null)
                {
                        if(PredicateLabel.compareTo(((StringKey)(entry.key)).getKey()) == 0)
                        {
                                //return already existing EID ( convert lid to EID)
                                predicateid = ((LabelHeapLeafData)(entry.data)).getData().returnPID();
                                scan.DestroyBTreeFileScan();
                                PredicateBT.close(); //Close the Predicate Btree file
                                return predicateid;
                        }
                }
                scan.DestroyBTreeFileScan();
                //Insert into Predicate HeapFile
                lid = PredicateHeap.insertLabel(PredicateLabel);
                //Insert into Predicate Btree file key,lid
                PredicateBT.insert(key,lid);
                predicateid = lid.returnPID();
                PredicateBT.close(); //Close the Predicate Btree file
        }
        catch(Exception e)
        {
                System.err.println (""+e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
        }
        return predicateid;
	}


	public boolean deletePredicate(String PredicateLabel)
	{
        boolean success = false;
        int KeyType = AttrType.attrString;
        KeyClass key = new StringKey(PredicateLabel);
        EID predicateid = null;

        //Open ENTITY BTree Index file
        try
        {
                PredicateHeap = new LabelHeapfile(dbname+"/predicateHF");
                PredicateBT = new LabelHeapBTreeFile(dbname+"/predicateBT");

                LID lid = null;
                KeyClass low_key = new StringKey(PredicateLabel);
                KeyClass high_key = new StringKey(PredicateLabel);
                KeyDataEntry entry = null;

                //Start Scanning BTree to check if entity already present
                LabelHeapBTFileScan scan = PredicateBT.new_scan(low_key,high_key);
                entry = scan.get_next();
                if(entry!=null)
                {
                        if(PredicateLabel.equals(((StringKey)(entry.key)).getKey()))
                        {
                                lid =  ((LabelHeapLeafData)entry.data).getData();
                                success = PredicateHeap.deleteLabel(lid) & PredicateBT.Delete(low_key,lid);
                        }
                }
                scan.DestroyBTreeFileScan();
                PredicateBT.close();
        }
        catch(Exception e)
        {
                System.err.println ("*** Error:predicate cannot be deleted" + e);
                e.printStackTrace();
        }
        
        return success;
	}

	public QID insertQuadruple(byte[] quadruplePtr)
	throws Exception
	{
		QID quadrupleid;
		QID qid = null;
		try
		{
			//Open Quadruple BTree Index file
			QuadrupleBT = new QuadrupleBTreeFile(dbname+"/quadrupleBT");
			int sub_slotNo = Convert.getIntValue(0,quadruplePtr);
			int sub_pageNo = Convert.getIntValue(4,quadruplePtr);
			int pred_slotNo = Convert.getIntValue(8,quadruplePtr); 
			int pred_pageNo = Convert.getIntValue(12,quadruplePtr);
			int obj_slotNo = Convert.getIntValue(16,quadruplePtr);
			int obj_pageNo = Convert.getIntValue(20,quadruplePtr);
			double confidence =Convert.getFloValue(24,quadruplePtr);
			String key = new String(Integer.toString(sub_slotNo) +':'+ Integer.toString(sub_pageNo) +':'+ Integer.toString(pred_slotNo) + ':' + Integer.toString(pred_pageNo) +':' + Integer.toString(obj_slotNo) +':'+ Integer.toString(obj_pageNo));
			KeyClass low_key = new StringKey(key);
			KeyClass high_key = new StringKey(key);
			KeyDataEntry entry = null;

			//Start Scaning Btree to check if  predicate already present
			QuadrupleBTFileScan scan = QuadrupleBT.new_scan(low_key,high_key);
			entry = scan.get_next();
			if(entry != null)
			{

				if(key.compareTo(((StringKey)(entry.key)).getKey()) == 0)
				{
					//return already existing QID 
					quadrupleid = ((QuadrupleLeafData)(entry.data)).getData();
					Quadruple record = QuadrupleHeap.getQuadruple(quadrupleid);
					double orig_confidence = record.getConfidence();
					if(orig_confidence > confidence)
					{
						Quadruple newRecord = new Quadruple(quadruplePtr,0);
						QuadrupleHeap.updateQuadruple(quadrupleid,newRecord);
					}       
					scan.DestroyBTreeFileScan();
					QuadrupleBT.close();
					return quadrupleid;
				}
			}

			//insert into quadruple heap file
			qid= QuadrupleHeap.insertQuadruple(quadruplePtr);


			//insert into quadruple btree
			QuadrupleBT.insert(low_key,qid);

			scan.DestroyBTreeFileScan();
			QuadrupleBT.close();
		}
		catch(Exception e)
		{
				System.err.println ("*** Error: Quadruple cannot be inserted " + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
		}

		return qid;
	}


	public boolean deleteQuadruple(byte[] Quadrupleptr)
	{
        boolean success = false;
        QID quadrupleid = null;
        try
        {
			//Open Quadruple BTree Index file
			QuadrupleBT = new QuadrupleBTreeFile(dbname+"/quadrupleBT");
			int sub_slotNo = Convert.getIntValue(0,Quadrupleptr);
			int sub_pageNo = Convert.getIntValue(4,Quadrupleptr);
			int pred_slotNo = Convert.getIntValue(8,Quadrupleptr);
			int pred_pageNo = Convert.getIntValue(12,Quadrupleptr);
			int obj_slotNo = Convert.getIntValue(16,Quadrupleptr);
			int obj_pageNo = Convert.getIntValue(20,Quadrupleptr);
			float confidence =Convert.getFloValue(24,Quadrupleptr);
			String key = new String(Integer.toString(sub_slotNo) +':'+ Integer.toString(sub_pageNo) +':'+ Integer.toString(pred_slotNo) + ':' + Integer.toString(pred_pageNo) +':' + Integer.toString(obj_slotNo) +':'+ Integer.toString(obj_pageNo));
			KeyClass low_key = new StringKey(key);
			KeyClass high_key = new StringKey(key);
			KeyDataEntry entry = null;

			//Start Scaning Btree to check if  predicate already present
			QuadrupleBTFileScan scan = QuadrupleBT.new_scan(low_key,high_key);
			entry = scan.get_next();
			if(entry != null)
			{
				if(key.compareTo(((StringKey)(entry.key)).getKey()) == 0)
				{
					//return already existing QID
					quadrupleid = ((QuadrupleLeafData)(entry.data)).getData();
					if(quadrupleid!=null)
						success = QuadrupleHeap.deleteQuadruple(quadrupleid);
				}
			}
			scan.DestroyBTreeFileScan();
			if(entry!=null)
			{
			if(low_key!=null && quadrupleid!=null)
			success = success & QuadrupleBT.Delete(low_key,quadrupleid);
			}

			QuadrupleBT.close();
                
        }
        catch(Exception e)
        {
                System.err.println ("*** Error:Quadruple cannot be deleted " + e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
        }
        
        return success;
	}

	
	//indexing
	public void createIndex(int type){
		switch(type){
			case 1:
			createIndex1();
			break;

			case 2:
			createIndex2();
			break;
			
			case 3:
			createIndex3();
			break;


			case 4:
			createIndex4();
			break;

			case 5:
			createIndex5();
			break;

			default:
				System.out.println("invalid option selected");
		}
    }

	public void createIndex1()
    {
		//Unclustered BTree on confidence using sorted Heap File
		try
		{
			//destroy existing index first
			File file = new File(dbname+"/Quadruple_BTreeIndex");
			if(QuadrupleBTIndex != null || file.exists())
			{
					System.out.println("Deleting the existing index btree.");
					QuadrupleBTIndex.close();
					QuadrupleBTIndex.destroyFile();
					destroyIndex(dbname+"/Quadruple_BTreeIndex");
			}

			//create new
			int keytype = AttrType.attrString;
			
			//scan sorted heap file and insert into btree index
			QuadrupleBTIndex = new QuadrupleBTreeFile(dbname+"/Quadruple_BTreeIndex");
			QuadrupleHeap = new QuadrupleHeapfile(dbname+"/quadrupleHF");
			TScan am = new TScan(QuadrupleHeap);
			Quadruple quadruple = null;
			QID qid = new QID();
			double confidence = 0.0;
			while((quadruple = am.getNext(qid)) != null)
			{
					confidence = quadruple.getConfidence();
					String temp = Double.toString(confidence);;
					KeyClass key = new StringKey(temp);
					QuadrupleBTIndex.insert(key,qid);

			}

			am.closescan();
			QuadrupleBTIndex.close();
		}
		catch(Exception e)
		{
			System.err.println ("*** Error: cannot create index1 " + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
	}

	public void createIndex2()
    {
		//Unclustered BTree Index file on subject and confidence
		try
		{
			//destroy existing index first
			if(QuadrupleBTIndex != null)
			{
					QuadrupleBTIndex.close();
					QuadrupleBTIndex.destroyFile();
					destroyIndex(dbname+"/Quadruple_BTreeIndex");
			}

			//create new
			int keytype = AttrType.attrString;
			
			//scan sorted heap file and insert into btree index
			QuadrupleBTIndex = new QuadrupleBTreeFile(dbname+"/Quadruple_BTreeIndex");
			QuadrupleHeap = new QuadrupleHeapfile(dbname+"/quadrupleHF");
			EntityHeap = new LabelHeapfile(dbname+"/entityHF");
			TScan am = new TScan(QuadrupleHeap);
			Quadruple quadruple = null;
			QID qid = new QID();
			double confidence = 0.0;
			while((quadruple = am.getNext(qid)) != null)
			{
				confidence = quadruple.getConfidence();
				String temp = Double.toString(confidence);
				Label subject = EntityHeap.getLabel(quadruple.getSubjecqid().returnLID());
				KeyClass key = new StringKey(subject.getLabel()+":"+temp);
				QuadrupleBTIndex.insert(key,qid);
			}
			am.closescan();
			QuadrupleBTIndex.close();
		}
		catch(Exception e)
		{
			System.err.println ("Error: cannot create index2" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

    }

	public void createIndex3()
    {
		//Unclustered BTree Index file on object and confidence
		try
		{
			//destroy existing index first
			if(QuadrupleBTIndex != null)
			{
				QuadrupleBTIndex.close();
				QuadrupleBTIndex.destroyFile();
				destroyIndex(dbname+"/Quadruple_BTreeIndex");
			}

			//create new
			int keytype = AttrType.attrString;
			
			
			//scan sorted heap file and insert into btree index
			QuadrupleBTIndex = new QuadrupleBTreeFile(dbname+"/Quadruple_BTreeIndex");
			QuadrupleHeap = new QuadrupleHeapfile(dbname+"/quadrupleHF");
			EntityHeap = new LabelHeapfile(dbname+"/entityHF");
			TScan am = new TScan(QuadrupleHeap);
			Quadruple quadruple = null;
			QID qid = new QID();
			double confidence = 0.0;
			while((quadruple = am.getNext(qid)) != null)
			{
				confidence = quadruple.getConfidence();
				String temp = Double.toString(confidence);
				Label object = EntityHeap.getLabel(quadruple.getObjecqid().returnLID());
				KeyClass key = new StringKey(object.getLabel()+":"+temp);
				QuadrupleBTIndex.insert(key,qid);
			}
			am.closescan();
			QuadrupleBTIndex.close();
		}
		catch(Exception e)
		{
			System.err.println ("Error: cannot create index3" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

    }

	public void createIndex4()
    {
		//Unclustered BTree Index file on predicate and confidence
		try
		{
			//destroy existing index first
			if(QuadrupleBTIndex != null)
			{
				QuadrupleBTIndex.close();
				QuadrupleBTIndex.destroyFile();
				destroyIndex(dbname+"/Quadruple_BTreeIndex");
			}

			//create new
			int keytype = AttrType.attrString;
			
			//scan sorted heap file and insert into btree index
			QuadrupleBTIndex = new QuadrupleBTreeFile(dbname+"/Quadruple_BTreeIndex");
			QuadrupleHeap = new QuadrupleHeapfile(dbname+"/quadrupleHF");
			PredicateHeap = new LabelHeapfile(dbname+"/predicateHF");
			TScan am = new TScan(QuadrupleHeap);
			Quadruple quadruple = null;
			QID qid = new QID();
			double confidence = 0.0;
			while((quadruple = am.getNext(qid)) != null)
			{
				confidence = quadruple.getConfidence();
				String temp = Double.toString(confidence);
				Label predicate = PredicateHeap.getLabel(quadruple.getPredicateID().returnLID());
				KeyClass key = new StringKey(predicate.getLabel()+":"+temp);
				QuadrupleBTIndex.insert(key,qid);
			}
			am.closescan();
			QuadrupleBTIndex.close();
		}
		catch(Exception e)
		{
			System.err.println ("Error: cannot create index4" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
    }

	public void createIndex5()
    {
		//Unclustered BTree Index file on subject
		try
		{
			//destroy existing index first
			if(QuadrupleBTIndex != null)
			{
				QuadrupleBTIndex.close();
				QuadrupleBTIndex.destroyFile();
				destroyIndex(dbname+"/Quadruple_BTreeIndex");
			}

			//create new
			int keytype = AttrType.attrString;
			//scan sorted heap file and insert into btree index
			QuadrupleBTIndex = new QuadrupleBTreeFile(dbname+"/Quadruple_BTreeIndex");
			QuadrupleHeap = new QuadrupleHeapfile(dbname+"/quadrupleHF");
			EntityHeap = new LabelHeapfile(dbname+"/entityHF");
			TScan am = new TScan(QuadrupleHeap);
			Quadruple quadruple = null;
			QID qid = new QID();
			KeyDataEntry entry = null;
			QuadrupleBTFileScan scan = null;
			
			while((quadruple = am.getNext(qid)) != null)
			{
				Label subject = EntityHeap.getLabel(quadruple.getSubjecqid().returnLID());
				KeyClass key = new StringKey(subject.getLabel());
				QuadrupleBTIndex.insert(key,qid);
			}
			am.closescan();
			QuadrupleBTIndex.close();
		}
		catch(Exception e)
		{
			System.err.println ("Error: cannot create index5" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
    }

	public void createIndexOnObject()
	{
		//Unclustered BTree Index file on object
		try
		{
			//destroy existing index first
			if(QuadrupleBTIndex != null)
			{
				QuadrupleBTIndex.close();
				QuadrupleBTIndex.destroyFile();
				destroyIndex(dbname+"/Object_BTreeIndex");
			}

			//create new
			int keytype = AttrType.attrString;
			//scan sorted heap file and insert into btree index
			QuadrupleBTIndex = new QuadrupleBTreeFile(dbname+"/Object_BTreeIndex");
			QuadrupleHeap = new QuadrupleHeapfile(dbname+"/quadrupleHF");
			EntityHeap = new LabelHeapfile(dbname+"/entityHF");
			TScan am = new TScan(QuadrupleHeap);
			Quadruple quadruple = null;
			QID qid = new QID();
			KeyDataEntry entry = null;
			QuadrupleBTFileScan scan = null;

			while((quadruple = am.getNext(qid)) != null)
			{
				Label object = EntityHeap.getLabel(quadruple.getObjecqid().returnLID());
				KeyClass key = new StringKey(object.getLabel());
				QuadrupleBTIndex.insert(key,qid);
			}
			am.closescan();
			QuadrupleBTIndex.close();
		}
		catch(Exception e)
		{
			System.err.println ("Error: cannot create index5" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
	}

	private void destroyIndex(String filename)
    {
		try
		{
			if(filename != null)
			{
					
				QuadrupleBTreeFile bfile = new QuadrupleBTreeFile(filename);
				
				QuadrupleBTFileScan scan = bfile.new_scan(null,null);
				QID qid = null;
				KeyDataEntry entry = null;
				ArrayList<KeyClass> keys = new ArrayList<KeyClass>();                   
				ArrayList<QID> qids = new ArrayList<QID>();
				int count = 0;                  

				while((entry = scan.get_next())!= null)
				{
					qid =  ((QuadrupleLeafData)entry.data).getData();
					keys.add(entry.key);
					qids.add(qid);
					count++;
				}
				scan.DestroyBTreeFileScan();

				for(int i = 0; i < count ;i++)
				{
					bfile.Delete(keys.get(i),qids.get(i));
				}

				bfile.close();  
			}
		}
		catch(GetFileEntryException e1)
		{
			System.out.println("no index at firsttime");
		}
		catch(Exception e)
		{
			System.err.println ("Error: cannot destory index " + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

    }

}
