/* File DB.java */

package diskmgr;

import java.io.*;
import java.util.ArrayList;
import java.util.TreeSet;

import bufmgr.*;
import global.*;
import labelheap.*;
import quadrupleheap.*;
import btree.*;

public class rdfDB extends DB implements GlobalConst {
  private QuadrupleHeapfile TEMP_Quadruple_HF;		//TEMPORARY HEAP FILE FOR SORTING
    
	private QuadrupleHeapfile Quadruple_HF; 	  		//Quadruples Heap file to store triples
	private LabelHeapfile Entity_HF; 	  		//Entity Heap file to store subjects/objects
	private LabelHeapfile Predicate_HF;   		//Predicates Heap file to store predicates

	private LabelHeapBTreeFile Entity_BTree;  		//BTree Index file on Entity Heap file
	private LabelHeapBTreeFile Predicate_BTree; 	//BTree Predicate file on Predicate Heap file
	private QuadrupleBTreeFile Quadruple_BTree; 		//BTree Predicate file on Predicate Heap file

	private String curr_dbname; 				//RDF Database name

	private int Total_Subjects = 0; 			//Total count of subjects in RDF
	private int Total_Objects = 0; 				//Total count of objects in RDF
	private int Total_Predicates = 0; 			//Total count of predicates in RDF
	private int Total_Quadruples = 0; 				//Total count of triples in RDF
	private int Total_Entities = 0;         	//Total count of entities in RDF

  private QuadrupleBTreeFile Quadruple_BTreeIndex; 	//BTree file for the index options given
	// INDEX OPTIONS	
	//(1) BTree Index file on confidence
	//(2) BTree Index file on subject and confidence
	//(3) BTree Index file on object and confidence
	//(4) BTree Index file on predicate and confidence
	//(5) BTree Index file on subject

  public QuadrupleHeapfile getTrpHandle() {
		// TODO Auto-generated method stub
		return Quadruple_HF;
	}

	public LabelHeapfile getEntityHandle() {
		// TODO Auto-generated method stub
		return Entity_HF;
	}
	public LabelHeapfile getPredicateHandle() {
		// TODO Auto-generated method stub
		return Predicate_HF;
	}
	
	public QuadrupleHeapfile getTEMP_Quadruple_HF() {
		return TEMP_Quadruple_HF;
	}

	public QuadrupleBTreeFile getQuadruple_BTreeIndex() 
	throws GetFileEntryException, PinPageException, ConstructPageException 
	{
		Quadruple_BTreeIndex = new QuadrupleBTreeFile(curr_dbname+"/Quadruple_BTreeIndex");
		return Quadruple_BTreeIndex;
	}

  public QuadrupleBTreeFile getQuadruple_BTree() 
	throws GetFileEntryException, PinPageException, ConstructPageException
	{
		Quadruple_BTree = new QuadrupleBTreeFile(curr_dbname+"/quadrupleBT");
		return Quadruple_BTree;
	}

  /**
	 * Default Constructor
	 */
	public rdfDB() { }

	/**
	* Close RdfDB
	*/
	public void rdfcloseDB() 
	throws 	PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException
	{
		try {

			if(Entity_BTree != null)
			{
				Entity_BTree.close();
				//Entity_BTree.destroyFile();
			}
			if(Predicate_BTree != null)
			{
				Predicate_BTree.close();
				//Predicate_BTree.destroyFile();
			}
			if(Quadruple_BTree != null)
			{
				Quadruple_BTree.close();
				//Triple_BTree.destroyFile();
			}
			if(Quadruple_BTreeIndex != null)
			{
				Quadruple_BTreeIndex.close();
				//Triple_BTreeIndex.destroyFile();
			}
			if(TEMP_Quadruple_HF != null && TEMP_Quadruple_HF != getTrpHandle())
			{
				TEMP_Quadruple_HF.deleteFile();
				//Triple_BTreeIndex.destroyFile();
			}
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

  
  /**Constructor for the RDF database. 
	 * @param type is an integer denoting the different clus-tering and indexing strategies you will use for the rdf database.   
	 * @Note: Each RDF database contains:
	 * one TripleHeapFile to store the triples,
	 * one LabelHeapfile to store entity labels, 
	 * and another LabelHeapfile to store subject labels. 
	 * You can create as many btree index files as you want over these triple and label heap files
	 */
	public rdfDB(int type) 
	{
		int keytype = AttrType.attrString;

		/** Initialize counter to zero **/ 
		PCounter.initialize();
		
		//Create TEMP TRIPLES heap file /TOFIX
		try
		{ 
			//System.out.println("Creating new TEMP triples heapfile");
			//TEMP_Triple_HF = new TripleHeapfile(Long.toString(System.currentTimeMillis()));
			TEMP_Quadruple_HF = new QuadrupleHeapfile("tempresult");
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
				
		//Create TRIPLES heap file
		try
		{ 
			//System.out.println("Creating new triples heapfile");
			Quadruple_HF = new QuadrupleHeapfile(curr_dbname+"/quadrupleHF");

		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		//Create ENTITES heap file: (Entity:Subject/Object)
		try
		{
			//System.out.println("Creating new entities heapfile");
			Entity_HF = new LabelHeapfile(curr_dbname+"/entityHF");
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		//Create PREDICATES heap file: (Predicates)
		try
		{
			//System.out.println("Creating new predicate heapfile");
			Predicate_HF = new LabelHeapfile(curr_dbname+"/predicateHF");
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
			Entity_BTree = new LabelHeapBTreeFile(curr_dbname+"/entityBT",keytype,255,1);
			Entity_BTree.close();
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
			Predicate_BTree = new LabelHeapBTreeFile(curr_dbname+"/predicateBT",keytype,255,1);
			Predicate_BTree.close();
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
				
		//Create Triple Binary tree file
		try
		{
			//System.out.println("Creating new Triple Binary Tree file");
			Quadruple_BTree = new QuadrupleBTreeFile(curr_dbname+"/quadrupleBT",keytype,255,1);
			Quadruple_BTree.close();
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		// try
		// {
		// 	//System.out.println("Creating new Label Binary Tree file for checking duplicate subjects");
		// 	dup_tree = new LabelHeapBTreeFile(curr_dbname+"/dupSubjBT",keytype,255,1);
		// 	dup_tree.close();
		// }
		// catch(Exception e)
		// {
		// 	System.err.println (""+e);
		// 	e.printStackTrace();
		// 	Runtime.getRuntime().exit(1);
		// }

		// try
		// {
		// 	//System.out.println("Creating new Label Binary Tree file for checking duplicate objects");
		// 	dup_Objtree = new LabelHeapBTreeFile(curr_dbname+"/dupObjBT",keytype,255,1);
		// 	dup_Objtree.close();
		// }
		// catch(Exception e)
		// {
		// 	System.err.println (""+e);
		// 	e.printStackTrace();
		// 	Runtime.getRuntime().exit(1);
		// }

		//Now create btree index files as per the index option
		try
		{
			//System.out.println("Creating Triple Binary Tree file for given index option");
			// Quadruple_BTreeIndex = new QuadrupleBTreeFile(curr_dbname+"/Quadruple_BTreeIndex",keytype,255,1);
			// Quadruple_BTreeIndex.close();
			createIndex(type);
		}
		catch(Exception e)
		{
			System.err.println ("Error creating B tree index for given index option"+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

	}

  /**
	 *  Get count of Triples in RDF DB
	 *  @return int number of Triples
	 */ 
	public int getQuadrupleCnt()
	{	
		try
		{
			Quadruple_HF = new QuadrupleHeapfile(curr_dbname+"/quadrupleHF");
			Total_Quadruples = Quadruple_HF.getQuadrupleCnt();
		}
		catch (Exception e) 
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		return Total_Quadruples;
	}

  /**
     *  Get count of Entities(unique) in RDF DB
     *  @return int number of distinct Entities
     */ 
    public int getEntityCnt()
    {
            try
            {
                    Entity_HF = new LabelHeapfile(curr_dbname+"/entityHF");
                    Total_Entities = Entity_HF.getLabelCnt();
            }
            catch (Exception e) 
            {
                    System.err.println (""+e);
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
            }
            return Total_Entities; 
    }

    /**
	 *  Get count of Predicates(unique) in RDF DB
	 *  @return int number of distinct Predicates
	 */ 
	public int getPredicateCnt()
	{
		try
		{
			Predicate_HF = new LabelHeapfile(curr_dbname+"/predicateHF");
			Total_Predicates = Predicate_HF.getLabelCnt();
		}
		catch (Exception e) 
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		return Total_Predicates; 
	}

  public int getSubjectCnt()
  {
      Total_Subjects = 0;
      KeyDataEntry entry = null;
      TreeSet<EID> subjectSet = new TreeSet<EID>((s1, s2) -> {
          if(s1.equals(s2)) return 0;
          return s1.pageNo.pid - s2.pageNo.pid; // to avoid skew tree
      });
      try
      {
          Quadruple_BTree = new QuadrupleBTreeFile(curr_dbname+"/quadrupleBT");
          QuadrupleBTFileScan scan = Quadruple_BTree.new_scan(null,null);
          do{
              entry = scan.get_next();
              if(entry!=null){
                  QID qid =  ((QuadrupleLeafData)entry.data).getData();
                  Quadruple quad = Quadruple_HF.getQuadruple(qid);
                  EID sub_id = quad.getSubjecqid();
        
                  subjectSet.add(sub_id);
              }
          }while(entry!=null);
          Total_Subjects = subjectSet.size();

          scan.DestroyBTreeFileScan();
          Quadruple_BTree.close();

      }
      catch(Exception e)
      {
          System.err.println (""+e);
          e.printStackTrace();
          Runtime.getRuntime().exit(1);
      }
      return Total_Subjects;
    }

    public int getObjectCnt()
    {
        Total_Objects = 0;
        KeyDataEntry entry = null;
        TreeSet<EID> objectSet = new TreeSet<EID>((o1, o2) -> {
            if(o1.equals(o2)) return 0;
            return o1.pageNo.pid - o2.pageNo.pid; // to avoid skew tree
        });
        try
        {
            Quadruple_BTree = new QuadrupleBTreeFile(curr_dbname+"/quadrupleBT");
            QuadrupleBTFileScan scan = Quadruple_BTree.new_scan(null,null);
            do{
                entry = scan.get_next();
                if(entry!=null){
                    QID qid =  ((QuadrupleLeafData)entry.data).getData();
                    Quadruple quad = Quadruple_HF.getQuadruple(qid);
                    EID obj_id = quad.getObjecqid();
                    objectSet.add(obj_id);
                }
            }while(entry!=null);
            Total_Objects = objectSet.size();
            scan.DestroyBTreeFileScan();
            Quadruple_BTree.close();

        }
        catch(Exception e)
        {
            System.err.println (""+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return Total_Objects;
    }

	/**
	 * Insert a entity into the EntityHeapFIle
	 * @param Entitylabel String representing Subject/Object
	 */
	public EID insertEntity(String EntityLabel) 
	{
	    int KeyType = AttrType.attrString;
        KeyClass key = new StringKey(EntityLabel);
        EID entityid = null;

        //Open ENTITY BTree Index file
        try
        {
			Entity_BTree = new LabelHeapBTreeFile(curr_dbname+"/entityBT");
			//      LabelBT.printAllLeafPages(Entity_BTree.getHeaderPage());

			LID lid = null;
			KeyClass low_key = new StringKey(EntityLabel);
			KeyClass high_key = new StringKey(EntityLabel);
			KeyDataEntry entry = null;

			//Start Scaning Btree to check if entity already present
			LabelHeapBTFileScan scan = Entity_BTree.new_scan(low_key,high_key);
			entry = scan.get_next();
			if(entry!=null)
			{
				if(EntityLabel.equals(((StringKey)(entry.key)).getKey()))
				{
						//return already existing EID ( convert lid to EID)
						lid =  ((LabelHeapLeafData)entry.data).getData();
						entityid = lid.returnEID();
						scan.DestroyBTreeFileScan();
						Entity_BTree.close();
						return entityid;
				}
			}

			scan.DestroyBTreeFileScan();
			//Insert into Entity HeapFile
			lid = Entity_HF.insertLabel(EntityLabel);   

			//Insert into Entity Btree file key,lid
			Entity_BTree.insert(key,lid); 

			entityid = lid.returnEID();
			Entity_BTree.close();
        }
        catch(Exception e)
        {
                System.err.println ("*** Error inserting entity ");
                e.printStackTrace();
        }

        return entityid; //Return EID
	}


	/**
	 * Delete a entity into the EntityHeapFile
	 * @param Entitylabel String representing Subject/Object
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
			Entity_HF = new LabelHeapfile(curr_dbname+"/entityHF");
			Entity_BTree = new LabelHeapBTreeFile(curr_dbname+"/entityBT");

			LID lid = null;
			KeyClass low_key = new StringKey(EntityLabel);
			KeyClass high_key = new StringKey(EntityLabel);
			KeyDataEntry entry = null;

			//Start Scaning Btree to check if entity already present
			LabelHeapBTFileScan scan = Entity_BTree.new_scan(low_key,high_key);
			entry = scan.get_next();
			if(entry!=null)
			{
				if(EntityLabel.equals(((StringKey)(entry.key)).getKey()))
				{
						//System.out.println(((StringKey)(entry.key)).getKey());        
						lid =  ((LabelHeapLeafData)entry.data).getData();
						success = Entity_HF.deleteLabel(lid) & Entity_BTree.Delete(low_key,lid);
				}
			}
			scan.DestroyBTreeFileScan();
			Entity_BTree.close();
        }
        catch(Exception e)
        {
                System.err.println ("*** Error deleting entity " + e);
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
                Predicate_BTree = new LabelHeapBTreeFile(curr_dbname+"/predicateBT"); 
                //LabelBT.printAllLeafPages(Predicate_BTree.getHeaderPage());
                KeyClass low_key = new StringKey(PredicateLabel);
                KeyClass high_key = new StringKey(PredicateLabel);
                KeyDataEntry entry = null;

                //Start Scaning Btree to check if  predicate already present
                LabelHeapBTFileScan scan = Predicate_BTree.new_scan(low_key,high_key);
                entry = scan.get_next();
                if(entry != null)
                {
                        if(PredicateLabel.compareTo(((StringKey)(entry.key)).getKey()) == 0)
                        {
                                //return already existing EID ( convert lid to EID)
                                predicateid = ((LabelHeapLeafData)(entry.data)).getData().returnPID();
                                scan.DestroyBTreeFileScan();
                                Predicate_BTree.close(); //Close the Predicate Btree file
                                return predicateid;
                        }
                }
                scan.DestroyBTreeFileScan();
                //Insert into Predicate HeapFile
                // lid = Predicate_HF.insertLabel(PredicateLabel.getBytes());
                lid = Predicate_HF.insertLabel(PredicateLabel);
                //Insert into Predicate Btree file key,lid
                Predicate_BTree.insert(key,lid); 
                predicateid = lid.returnPID();
                Predicate_BTree.close(); //Close the Predicate Btree file
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
                Predicate_HF = new LabelHeapfile(curr_dbname+"/predicateHF");
                Predicate_BTree = new LabelHeapBTreeFile(curr_dbname+"/predicateBT");
                //      LabelBT.printAllLeafPages(Entity_BTree.getHeaderPage());

                LID lid = null;
                KeyClass low_key = new StringKey(PredicateLabel);
                KeyClass high_key = new StringKey(PredicateLabel);
                KeyDataEntry entry = null;

                //Start Scanning BTree to check if entity already present
                LabelHeapBTFileScan scan = Predicate_BTree.new_scan(low_key,high_key);
                entry = scan.get_next();
                if(entry!=null)
                {
                        if(PredicateLabel.equals(((StringKey)(entry.key)).getKey()))
                        {
                                //System.out.println(((StringKey)(entry.key)).getKey());        
                                lid =  ((LabelHeapLeafData)entry.data).getData();
                                success = Predicate_HF.deleteLabel(lid) & Predicate_BTree.Delete(low_key,lid);
                        }
                }
                scan.DestroyBTreeFileScan();
                Predicate_BTree.close();
        }
        catch(Exception e)
        {
                System.err.println ("*** Error deleting predicate " + e);
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
			//Open Triple BTree Index file
			Quadruple_BTree = new QuadrupleBTreeFile(curr_dbname+"/quadrupleBT"); 
			//TripleBT.printAllLeafPages(Triple_BTree.getHeaderPage());
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
			QuadrupleBTFileScan scan = Quadruple_BTree.new_scan(low_key,high_key);
			entry = scan.get_next();
			if(entry != null)
			{
				//System.out.println("Duplicate Triple found : " + ((StringKey)(entry.key)).getKey());
				if(key.compareTo(((StringKey)(entry.key)).getKey()) == 0)
				{
					//return already existing QID 
					quadrupleid = ((QuadrupleLeafData)(entry.data)).getData();
					Quadruple record = Quadruple_HF.getQuadruple(quadrupleid);
					double orig_confidence = record.getConfidence();
					if(orig_confidence > confidence)
					{
						Quadruple newRecord = new Quadruple(quadruplePtr,0);
						Quadruple_HF.updateQuadruple(quadrupleid,newRecord);
					}       
					scan.DestroyBTreeFileScan();
					Quadruple_BTree.close();
					return quadrupleid;
				}
			}

			//insert into quadruple heap file
			//System.out.println("("+quadruplePtr+")");
			qid= Quadruple_HF.insertQuadruple(quadruplePtr);

			//System.out.println("Inserting triple key : "+ key + "tid : " + tid);
			//insert into quadruple btree
			Quadruple_BTree.insert(low_key,qid); 

			scan.DestroyBTreeFileScan();
			Quadruple_BTree.close();
		}
		catch(Exception e)
		{
				System.err.println ("*** Error inserting quadruple record " + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
		}

		return qid;
	}


	public boolean deleteQuadruple(byte[] triplePtr)
	{
        boolean success = false;
        QID quadrupleid = null;
        try
        {
			//Open Quadruple BTree Index file
			Quadruple_BTree = new QuadrupleBTreeFile(curr_dbname+"/quadrupleBT"); 
			//TripleBT.printAllLeafPages(Triple_BTree.getHeaderPage());
			int sub_slotNo = Convert.getIntValue(0,triplePtr);
			int sub_pageNo = Convert.getIntValue(4,triplePtr);
			int pred_slotNo = Convert.getIntValue(8,triplePtr); 
			int pred_pageNo = Convert.getIntValue(12,triplePtr);
			int obj_slotNo = Convert.getIntValue(16,triplePtr);
			int obj_pageNo = Convert.getIntValue(20,triplePtr);
			float confidence =Convert.getFloValue(24,triplePtr);
			String key = new String(Integer.toString(sub_slotNo) +':'+ Integer.toString(sub_pageNo) +':'+ Integer.toString(pred_slotNo) + ':' + Integer.toString(pred_pageNo) +':' + Integer.toString(obj_slotNo) +':'+ Integer.toString(obj_pageNo));
			//System.out.println(key);
			KeyClass low_key = new StringKey(key);
			KeyClass high_key = new StringKey(key);
			KeyDataEntry entry = null;

			//Start Scaning Btree to check if  predicate already present
			QuadrupleBTFileScan scan = Quadruple_BTree.new_scan(low_key,high_key);
			entry = scan.get_next();
			if(entry != null)
			{
				//System.out.println("Triple found : " + ((StringKey)(entry.key)).getKey());
				if(key.compareTo(((StringKey)(entry.key)).getKey()) == 0)
				{
					//return already existing TID 
					quadrupleid = ((QuadrupleLeafData)(entry.data)).getData();
					if(quadrupleid!=null)
						success = Quadruple_HF.deleteQuadruple(quadrupleid); 
				}
			}
			scan.DestroyBTreeFileScan();
			if(entry!=null)
			{
			if(low_key!=null && quadrupleid!=null)
			success = success & Quadruple_BTree.Delete(low_key,quadrupleid);
			}

			Quadruple_BTree.close();
                
        }
        catch(Exception e)
        {
                System.err.println ("*** Error deleting quadruple record " + e);
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
		}
    }

	public void createIndex1()
    {
		//Unclustered BTree on confidence using sorted Heap File
		try
		{
			//destroy existing index first
			if(Quadruple_BTreeIndex != null)
			{
					Quadruple_BTreeIndex.close();
					Quadruple_BTreeIndex.destroyFile();
					destroyIndex(curr_dbname+"/Quadruple_BTreeIndex");
			}

			//create new
			int keytype = AttrType.attrString;
			Quadruple_BTreeIndex = new QuadrupleBTreeFile(curr_dbname+"/Quadruple_BTreeIndex",keytype,255,1);
			Quadruple_BTreeIndex.close();
			
			//scan sorted heap file and insert into btree index
			Quadruple_BTreeIndex = new QuadrupleBTreeFile(curr_dbname+"/Quadruple_BTreeIndex"); 
			Quadruple_HF = new QuadrupleHeapfile(curr_dbname+"/quadrupleHF");
			TScan am = new TScan(Quadruple_HF);
			Quadruple quadruple = null;
			QID qid = new QID();
			double confidence = 0.0;
			while((quadruple = am.getNext(qid)) != null)
			{
					confidence = quadruple.getConfidence();
					String temp = Double.toString(confidence);
					KeyClass key = new StringKey(temp);
					//System.out.println("Inserting into Btree key"+ temp + " tid "+tid);
					Quadruple_BTreeIndex.insert(key,qid); 
					//System.out.println("Inserting into Btree key"+ temp + " tid "+tid);
					
			}
			/*
			TripleBTFileScan scan = Triple_BTreeIndex.new_scan(null,null);
			KeyDataEntry entry = null;
			while((entry = scan.get_next())!= null)
			{
					System.out.println("Triple found : " + ((StringKey)(entry.key)).getKey());
			}
			scan.DestroyBTreeFileScan();
			*/
			am.closescan();
			Quadruple_BTreeIndex.close();
		}
		catch(Exception e)
		{
			System.err.println ("*** Error creating Index for option1 " + e);
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
			if(Quadruple_BTreeIndex != null)
			{
					Quadruple_BTreeIndex.close();
					Quadruple_BTreeIndex.destroyFile();
					destroyIndex(curr_dbname+"/Quadruple_BTreeIndex");
			}

			//create new
			int keytype = AttrType.attrString;
			Quadruple_BTreeIndex = new QuadrupleBTreeFile(curr_dbname+"/Quadruple_BTreeIndex",keytype,255,1);
			Quadruple_BTreeIndex.close();
			
			//scan sorted heap file and insert into btree index
			Quadruple_BTreeIndex = new QuadrupleBTreeFile(curr_dbname+"/Quadruple_BTreeIndex"); 
			Quadruple_HF = new QuadrupleHeapfile(curr_dbname+"/quadrupleHF");
			Entity_HF = new LabelHeapfile(curr_dbname+"/entityHF");
			TScan am = new TScan(Quadruple_HF);
			Quadruple quadruple = null;
			QID qid = new QID();
			double confidence = 0.0;
			while((quadruple = am.getNext(qid)) != null)
			{
				confidence = quadruple.getConfidence();
				String temp = Double.toString(confidence);
				Label subject = Entity_HF.getLabel(quadruple.getSubjecqid().returnLID());
				// String subject = Entity_HF.getLabel(quadruple.getSubjecqid().returnLID());
				//System.out.println("Subject--> "+subject.getLabelKey());
				KeyClass key = new StringKey(subject.getLabel()+":"+temp);
				//System.out.println("Inserting into Btree key"+ subject.getLabelKey() + ":" + temp + " tid "+tid);
				Quadruple_BTreeIndex.insert(key,qid); 
			}
			/*
			TripleBTFileScan scan = Triple_BTreeIndex.new_scan(null,null);
			KeyDataEntry entry = null;
			while((entry = scan.get_next())!= null)
			{
					System.out.println("Key found : " + ((StringKey)(entry.key)).getKey());
			}
			scan.DestroyBTreeFileScan();
			*/
			am.closescan();
			Quadruple_BTreeIndex.close();
		}
		catch(Exception e)
		{
			System.err.println ("*** Error creating Index for option2 " + e);
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
			if(Quadruple_BTreeIndex != null)
			{
				Quadruple_BTreeIndex.close();
				Quadruple_BTreeIndex.destroyFile();
				destroyIndex(curr_dbname+"/Quadruple_BTreeIndex");
			}

			//create new
			int keytype = AttrType.attrString;
			Quadruple_BTreeIndex = new QuadrupleBTreeFile(curr_dbname+"/Quadruple_BTreeIndex",keytype,255,1);
			Quadruple_BTreeIndex.close();
			
			//scan sorted heap file and insert into btree index
			Quadruple_BTreeIndex = new QuadrupleBTreeFile(curr_dbname+"/Quadruple_BTreeIndex"); 
			Quadruple_HF = new QuadrupleHeapfile(curr_dbname+"/QuadrupleHF");
			Entity_HF = new LabelHeapfile(curr_dbname+"/entityHF");
			TScan am = new TScan(Quadruple_HF);
			Quadruple quadruple = null;
			QID qid = new QID();
			double confidence = 0.0;
			while((quadruple = am.getNext(qid)) != null)
			{
				confidence = quadruple.getConfidence();
				String temp = Double.toString(confidence);
				Label object = Entity_HF.getLabel(quadruple.getObjecqid().returnLID());
				// String object = Entity_HF.getLabel(quadruple.getObjecqid().returnLID());
				//System.out.println("Subject--> "+subject.getLabelKey());
				KeyClass key = new StringKey(object.getLabel()+":"+temp);
				//System.out.println("Inserting into Btree key"+ object.getLabelKey() + ":" + temp + " tid "+tid);
				Quadruple_BTreeIndex.insert(key,qid); 
			}
			/*
			TripleBTFileScan scan = Triple_BTreeIndex.new_scan(null,null);
			KeyDataEntry entry = null;
			while((entry = scan.get_next())!= null)
			{
					System.out.println("Key found : " + ((StringKey)(entry.key)).getKey());
			}
			scan.DestroyBTreeFileScan();
			*/
			am.closescan();
			Quadruple_BTreeIndex.close();
		}
		catch(Exception e)
		{
			System.err.println ("*** Error creating Index for option3 " + e);
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
			if(Quadruple_BTreeIndex != null)
			{
				Quadruple_BTreeIndex.close();
				Quadruple_BTreeIndex.destroyFile();
				destroyIndex(curr_dbname+"/Quadruple_BTreeIndex");
			}

			//create new
			int keytype = AttrType.attrString;
			Quadruple_BTreeIndex = new QuadrupleBTreeFile(curr_dbname+"/Quadruple_BTreeIndex",keytype,255,1);
			Quadruple_BTreeIndex.close();
			
			//scan sorted heap file and insert into btree index
			Quadruple_BTreeIndex = new QuadrupleBTreeFile(curr_dbname+"/Quadruple_BTreeIndex"); 
			Quadruple_HF = new QuadrupleHeapfile(curr_dbname+"/quadrupleHF");
			Predicate_HF = new LabelHeapfile(curr_dbname+"/predicateHF");
			TScan am = new TScan(Quadruple_HF);
			Quadruple quadruple = null;
			QID qid = new QID();
			double confidence = 0.0;
			while((quadruple = am.getNext(qid)) != null)
			{
				confidence = quadruple.getConfidence();
				String temp = Double.toString(confidence);
				Label predicate = Predicate_HF.getLabel(quadruple.getPredicateID().returnLID());
				// String predicate = Predicate_HF.getLabel(quadruple.getPredicateID().returnLID());
				//System.out.println("Subject--> "+subject.getLabelKey());
				KeyClass key = new StringKey(predicate.getLabel()+":"+temp);
				//System.out.println("Inserting into Btree key"+ predicate.getLabelKey() + ":" + temp + " tid "+tid);
				Quadruple_BTreeIndex.insert(key,qid); 
			}
			/*
			TripleBTFileScan scan = Triple_BTreeIndex.new_scan(null,null);
			KeyDataEntry entry = null;
			while((entry = scan.get_next())!= null)
			{
					System.out.println("Key found : " + ((StringKey)(entry.key)).getKey());
			}
			scan.DestroyBTreeFileScan();
			*/
			am.closescan();
			Quadruple_BTreeIndex.close();
		}
		catch(Exception e)
		{
			System.err.println ("*** Error creating Index for option4 " + e);
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
			if(Quadruple_BTreeIndex != null)
			{
				Quadruple_BTreeIndex.close();
				Quadruple_BTreeIndex.destroyFile();
				destroyIndex(curr_dbname+"/Quadruple_BTreeIndex");	
			}

			//create new
			int keytype = AttrType.attrString;
			Quadruple_BTreeIndex = new QuadrupleBTreeFile(curr_dbname+"/Quadruple_BTreeIndex",keytype,255,1);
			
			//scan sorted heap file and insert into btree index
			Quadruple_HF = new QuadrupleHeapfile(curr_dbname+"/quadrupleHF");
			Entity_HF = new LabelHeapfile(curr_dbname+"/entityHF");
			TScan am = new TScan(Quadruple_HF);
			Quadruple quadruple = null;
			QID qid = new QID();
			KeyDataEntry entry = null;
			QuadrupleBTFileScan scan = null;
			
			/*TripleBTFileScan scan = Triple_BTreeIndex.new_scan(null,null);
			while((entry = scan.get_next())!= null)
			{
					System.out.println("Key found : " + ((StringKey)(entry.key)).getKey());
			}
			scan.DestroyBTreeFileScan();
			*/

			while((quadruple = am.getNext(qid)) != null)
			{
				Label subject = Entity_HF.getLabel(quadruple.getSubjecqid().returnLID());
				// String subject = Entity_HF.getLabel(quadruple.getSubjecqid().returnLID());
				KeyClass key = new StringKey(subject.getLabel());
				//     entry = null;

					//Start Scanning Btree to check if subject already present
				//     scan = Triple_BTreeIndex.new_scan(key,key);
				//     entry = scan.get_next();
				//     if(entry == null)
				//     {
				Quadruple_BTreeIndex.insert(key,qid); 
							//System.out.println("Inserting into Btree key"+ subject.getLabelKey() + " tid "+tid);
				//     }
				//     else
				//             System.out.println("Duplicate subject found "+ subject.getLabelKey() + " tid "+tid);
							
				//      scan.DestroyBTreeFileScan();
			}
			/*
			scan = Triple_BTreeIndex.new_scan(null,null);
			entry = null;
			while((entry = scan.get_next())!= null)
			{
					System.out.println("Key found : " + ((StringKey)(entry.key)).getKey());
			}
			scan.DestroyBTreeFileScan();
			*/
			am.closescan();
			Quadruple_BTreeIndex.close();
		}
		catch(Exception e)
		{
			System.err.println ("*** Error creating Index for option5 " + e);
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
					//System.out.println("Deleting record having Key : " + keys.get(i) + " TID " + tids.get(i));
					bfile.Delete(keys.get(i),qids.get(i));
				}

				bfile.close();  
			}
		}
		catch(GetFileEntryException e1)
		{
			System.out.println("Firsttime No index present.. Expected");
		}
		catch(Exception e)
		{
			System.err.println ("*** Error destroying Index " + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

    }

}

