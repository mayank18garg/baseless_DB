/* File DB.java */

package diskmgr;

import btree.*;
import global.*;
import quadrupleheap.Quadruple;
import quadrupleheap.QuadrupleHeapfile;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class rdfDB implements GlobalConst {


  private static final int bits_per_page = MAX_SPACE * 8;
    private QuadrupleHeapfile TQuadrupleHF;		//TEMPORARY HEAP FILE FOR SORTING

    private QuadrupleHeapfile QuadrupleHF; 	  		//Triples Heap file to store triples
    private LabelHeapfile Entity_HF; 	  		//Entity Heap file to store subjects/objects
    private LabelHeapfile Predicate_HF;   		//Predicates Heap file to store predicates

    private LabelBTreeFile Entity_BTree;  		//BTree Index file on Entity Heap file
    private LabelBTreeFile Predicate_BTree; 	//BTree Predicate file on Predicate Heap file
    private QuadrupleBTreeFile QuadrupleBTree; 		//BTree Predicate file on Predicate Heap file

    private String usedbname; 				//RDF Database name

    private LabelBTreeFile dup_tree;        	//BTree file for duplicate subjects
    private LabelBTreeFile dup_Objtree;     	//BTree file for duplicate objects

    private int Total_Subjects = 0; 			//Total count of subjects in RDF
    private int Total_Objects = 0; 				//Total count of objects in RDF
    private int Total_Predicates = 0; 			//Total count of predicates in RDF
    private int Total_Quadruples = 0; 				//Total count of triples in RDF
    private int Total_Entities = 0;         	//Total count of entities in RDF


    private QuadrupleBTreeFile QuadrupleBTreeIndex; 	//BTree file for the index options given
    // INDEX OPTIONS
    //(1) BTree Index file on confidence
    //(2) BTree Index file on subject and confidence
    //(3) BTree Index file on object and confidence
    //(4) BTree Index file on predicate and confidence
    //(5) BTree Index file on subject

  /** Open the database with the given name.
   *
   * @param name DB_name
   *
   * @exception IOException I/O errors
   * @exception FileIOException file I/O error
   * @exception InvalidPageNumberException invalid page number
   * @exception DiskMgrException error caused by other layers
   */
  public void openDB( String fname)
    throws IOException,
	   InvalidPageNumberException,
	   FileIOException,
	   DiskMgrException {

    name = fname;

    // Creaat a random access file
    fp = new RandomAccessFile(fname, "rw");

    PageId pageId = new PageId();
    Page apage = new Page();
    pageId.pid = 0;

    num_pages = 1;	//temporary num_page value for pinpage to work

    pinPage(pageId, apage, false /*read disk*/);


    DBFirstPage firstpg = new DBFirstPage();
    firstpg.openPage(apage);
    num_pages = firstpg.getNumDBPages();

    unpinPage(pageId, false /* undirty*/);
  }

  /** default constructor.
   */
  public rdfDB() { }
  
  
  /** DB Constructors.
   * Create a database with the specified number of pages where the page
   * size is the default page size.
   *
   * @param name DB name
   * @param num_pages number of pages in DB
   *
   * @exception IOException I/O errors
   * @exception InvalidPageNumberException invalid page number
   * @exception FileIOException file I/O error
   * @exception DiskMgrException error caused by other layers
   */
  public void openDB( String fname, int num_pgs)
    throws IOException, 
	   InvalidPageNumberException,
	   FileIOException,
	   DiskMgrException {
    
    name = new String(fname);
    num_pages = (num_pgs > 2) ? num_pgs : 2;
    
    File DBfile = new File(name);
    
    DBfile.delete();
    
    // Creaat a random access file
    fp = new RandomAccessFile(fname, "rw");
    
    // Make the file num_pages pages long, filled with zeroes.
    fp.seek((long)(num_pages*MINIBASE_PAGESIZE-1));
    fp.writeByte(0);
    
    // Initialize space map and directory pages.
    
    // Initialize the first DB page
    Page apage = new Page();
    PageId pageId = new PageId();
    pageId.pid = 0;
    pinPage(pageId, apage, true /*no diskIO*/);
    
    DBFirstPage firstpg = new DBFirstPage(apage);
    
    firstpg.setNumDBPages(num_pages);
    unpinPage(pageId, true /*dirty*/);
    
    // Calculate how many pages are needed for the space map.  Reserve pages
    // 0 and 1 and as many additional pages for the space map as are needed.
    int num_map_pages = (num_pages + bits_per_page -1)/bits_per_page;
    
    set_bits(pageId, 1+num_map_pages, 1);
    
  }
  
  /** Close DB file.
   * @exception IOException I/O errors.
   */
  public void closeDB() throws IOException {
    fp.close();
  }
  
  
  /** Destroy the database, removing the file that stores it. 
   * @exception IOException I/O errors.
   */
  public void DBDestroy() 
    throws IOException {
    
    fp.close();
    File DBfile = new File(name);
    DBfile.delete();
  }
  
  /** Read the contents of the specified page into a Page object
   *
   * @param pageno pageId which will be read
   * @param apage page object which holds the contents of page
   *
   * @exception InvalidPageNumberException invalid page number
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   */
  public  void read_page(PageId pageno, Page apage)
    throws InvalidPageNumberException, 
	   FileIOException, 
	   IOException {

    if((pageno.pid < 0)||(pageno.pid >= num_pages))
      throw new InvalidPageNumberException(null, "BAD_PAGE_NUMBER");
    
    // Seek to the correct page
    fp.seek((long)(pageno.pid *MINIBASE_PAGESIZE));
    
    // Read the appropriate number of bytes.
    byte [] buffer = apage.getpage();  //new byte[MINIBASE_PAGESIZE];
    try{
      fp.read(buffer);
    }
    catch (IOException e) {
      throw new FileIOException(e, "DB file I/O error");
    }
    
  }
  
  /** Write the contents in a page object to the specified page.
   *
   * @param pageno pageId will be wrote to disk
   * @param apage the page object will be wrote to disk
   *
   * @exception InvalidPageNumberException invalid page number
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   */
  public void write_page(PageId pageno, Page apage)
    throws InvalidPageNumberException, 
	   FileIOException, 
	   IOException {

    if((pageno.pid < 0)||(pageno.pid >= num_pages))
      throw new InvalidPageNumberException(null, "INVALID_PAGE_NUMBER");
    
    // Seek to the correct page
    fp.seek((long)(pageno.pid *MINIBASE_PAGESIZE));
    
    // Write the appropriate number of bytes.
    try{
      fp.write(apage.getpage());
    }
    catch (IOException e) {
      throw new FileIOException(e, "DB file I/O error");
    }
    
  }
  
  /** Allocate a set of pages where the run size is taken to be 1 by default.
   *  Gives back the page number of the first page of the allocated run.
   *  with default run_size =1
   *
   * @param start_page_num page number to start with 
   *
   * @exception OutOfSpaceException database is full
   * @exception InvalidRunSizeException invalid run size 
   * @exception InvalidPageNumberException invalid page number
   * @exception FileIOException DB file I/O errors
   * @exception IOException I/O errors
   * @exception DiskMgrException error caused by other layers
   */
  public void allocate_page(PageId start_page_num)
    throws OutOfSpaceException, 
	   InvalidRunSizeException, 
	   InvalidPageNumberException, 
	   FileIOException, 
	   DiskMgrException,
           IOException {
    allocate_page(start_page_num, 1);
  }
  
  /** user specified run_size
   *
   * @param start_page_num the starting page id of the run of pages
   * @param run_size the number of page need allocated
   *
   * @exception OutOfSpaceException No space left
   * @exception InvalidRunSizeException invalid run size 
   * @exception InvalidPageNumberException invalid page number
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   * @exception DiskMgrException error caused by other layers
   */
  public void allocate_page(PageId start_page_num, int runsize)
    throws OutOfSpaceException, 
	   InvalidRunSizeException, 
	   InvalidPageNumberException, 
	   FileIOException, 
	   DiskMgrException,
           IOException {

    if(runsize < 0) throw new InvalidRunSizeException(null, "Negative run_size");
    
    int run_size = runsize;
    int num_map_pages = (num_pages + bits_per_page -1)/bits_per_page;
    int current_run_start = 0; 
    int current_run_length = 0;
    
    
    // This loop goes over each page in the space map.
    PageId pgid = new PageId();
    byte [] pagebuf;
    int byteptr;
    
    for(int i=0; i< num_map_pages; ++i) {// start forloop01
	
      pgid.pid = 1 + i;
      // Pin the space-map page.
      
      Page apage = new Page();
      pinPage(pgid, apage, false /*read disk*/);
      
      pagebuf = apage.getpage();
      byteptr = 0;
      
      // get the num of bits on current page
      int num_bits_this_page = num_pages - i*bits_per_page;
      if(num_bits_this_page > bits_per_page)
	num_bits_this_page = bits_per_page;
      
      // Walk the page looking for a sequence of 0 bits of the appropriate
      // length.  The outer loop steps through the page's bytes, the inner
      // one steps through each byte's bits.
      
      for(; num_bits_this_page>0 
	    && current_run_length < run_size; ++byteptr) {// start forloop02
	  
	
	Integer intmask = new Integer(1);
	Byte mask = new Byte(intmask.byteValue());
	byte tmpmask = mask.byteValue();
	
	while (mask.intValue()!=0 && (num_bits_this_page>0)
	       &&(current_run_length < run_size))
	  
	  {	      
	    if( (pagebuf[byteptr] & tmpmask ) != 0)
	      {
		current_run_start += current_run_length + 1;
		current_run_length = 0;
	      }
	    else ++current_run_length;
	    
	    
	    tmpmask <<=1;
	    mask = new Byte(tmpmask);
	    --num_bits_this_page;
	  }
	
	
      }//end of forloop02
      // Unpin the space-map page.
      
      unpinPage(pgid, false /*undirty*/);
      
    }// end of forloop01
    
    if(current_run_length >= run_size)
      {
	start_page_num.pid = current_run_start;
	set_bits(start_page_num, run_size, 1);
	
	return;
      }
    
    throw new OutOfSpaceException(null, "No space left");
  }
  
  /** Deallocate a set of pages starting at the specified page number and
   * a run size can be specified.
   *
   * @param start_page_num the start pageId to be deallocate
   * @param run_size the number of pages to be deallocated
   * 
   * @exception InvalidRunSizeException invalid run size 
   * @exception InvalidPageNumberException invalid page number
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   * @exception DiskMgrException error caused by other layers
   */
  public void deallocate_page(PageId start_page_num, int run_size)
    throws InvalidRunSizeException, 
	   InvalidPageNumberException, 
	   IOException, 
	   FileIOException,
	   DiskMgrException {

    if(run_size < 0) throw new InvalidRunSizeException(null, "Negative run_size");
    
    set_bits(start_page_num, run_size, 0);
  }
  
  /** Deallocate a set of pages starting at the specified page number
   *  with run size = 1
   *
   * @param start_page_num the start pageId to be deallocate
   * @param run_size the number of pages to be deallocated
   *
   * @exception InvalidRunSizeException invalid run size 
   * @exception InvalidPageNumberException invalid page number
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   * @exception DiskMgrException error caused by other layers
   * 
   */
  public void deallocate_page(PageId start_page_num)
    throws InvalidRunSizeException, 
	   InvalidPageNumberException, 
	   IOException, 
	   FileIOException,
	   DiskMgrException {

    set_bits(start_page_num, 1, 0);
  }
  
  /** Adds a file entry to the header page(s).
   *
   * @param fname file entry name
   * @param start_page_num the start page number of the file entry
   *
   * @exception FileNameTooLongException invalid file name (too long)
   * @exception InvalidPageNumberException invalid page number
   * @exception InvalidRunSizeException invalid DB run size
   * @exception DuplicateEntryException entry for DB is not unique
   * @exception OutOfSpaceException database is full
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   * @exception DiskMgrException error caused by other layers
   */
  public void add_file_entry(String fname, PageId start_page_num)
    throws FileNameTooLongException, 
	   InvalidPageNumberException, 
	   InvalidRunSizeException,
	   DuplicateEntryException,
	   OutOfSpaceException,
	   FileIOException, 
	   IOException, 
	   DiskMgrException {
    
    if(fname.length() >= MAX_NAME)
      throw new FileNameTooLongException(null, "DB filename too long");
    if((start_page_num.pid < 0)||(start_page_num.pid >= num_pages))
      throw new InvalidPageNumberException(null, " DB bad page number");
    
    // Does the file already exist?  
    
    if( get_file_entry(fname) != null) 
      throw new DuplicateEntryException(null, "DB fileentry already exists");
    
    Page apage = new Page();
    
    boolean found = false;
    int free_slot = 0;
    PageId hpid = new PageId();
    PageId nexthpid = new PageId(0);
    DBHeaderPage dp;
    do
      {// Start DO01
	//  System.out.println("start do01");
        hpid.pid = nexthpid.pid;
	
	// Pin the header page
	pinPage(hpid, apage, false /*read disk*/);
        
	// This complication is because the first page has a different
        // structure from that of subsequent pages.
	if(hpid.pid==0)
	  {
	    dp = new DBFirstPage();
	    ((DBFirstPage) dp).openPage(apage);
	  }
	else
	  {
	    dp = new DBDirectoryPage();
	    ((DBDirectoryPage) dp).openPage(apage);
	  }
	
	nexthpid = dp.getNextPage();
	int entry = 0;
	
	PageId tmppid = new PageId();
	while(entry < dp.getNumOfEntries())
	  {
	    dp.getFileEntry(tmppid, entry);
	    if(tmppid.pid == INVALID_PAGE)  break;
	    entry ++;
	  }
	
	if(entry < dp.getNumOfEntries())
	  {
	    free_slot = entry;
	    found = true;
	  }
	else if (nexthpid.pid != INVALID_PAGE)
	  {
	    // We only unpin if we're going to continue looping.
	    unpinPage(hpid, false /* undirty*/);
	  }
	
      }while((nexthpid.pid != INVALID_PAGE)&&(!found)); // End of DO01
    
    // Have to add a new header page if possible.
    if(!found)
      {
	try{
	  allocate_page(nexthpid);
	}
	catch(Exception e){         //need rethrow an exception!!!!
          unpinPage(hpid, false /* undirty*/);
	  e.printStackTrace();
	}
	
	// Set the next-page pointer on the previous directory page.
	dp.setNextPage(nexthpid);
	unpinPage(hpid, true /* dirty*/);
	
	// Pin the newly-allocated directory page.
	hpid.pid = nexthpid.pid;
	
	pinPage(hpid, apage, true/*no diskIO*/);
	dp = new DBDirectoryPage(apage);
	
	free_slot = 0;
      }
    
    // At this point, "hpid" has the page id of the header page with the free
    // slot; "pg" points to the pinned page; "dp" has the directory_page
    // pointer; "free_slot" is the entry number in the directory where we're
    // going to put the new file entry.
    
    dp.setFileEntry(start_page_num, fname, free_slot);
    
    unpinPage(hpid, true /* dirty*/);
    
  }
  
  /** Delete the entry corresponding to a file from the header page(s).
   *
   * @param fname file entry name
   *
   * @exception FileEntryNotFoundException file does not exist
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   * @exception InvalidPageNumberException invalid page number
   * @exception DiskMgrException error caused by other layers
   */
  public void delete_file_entry(String fname)
    throws FileEntryNotFoundException, 
	   IOException,
	   FileIOException,
	   InvalidPageNumberException, 
	   DiskMgrException {
    
    Page apage = new Page();
    boolean found = false;
    int slot = 0;
    PageId hpid = new PageId();
    PageId nexthpid = new PageId(0);
    PageId tmppid = new PageId();
    DBHeaderPage dp;
    
    do
      { // startDO01
        hpid.pid = nexthpid.pid;
	
	// Pin the header page.
	pinPage(hpid, apage, false/*read disk*/);

	// This complication is because the first page has a different
        // structure from that of subsequent pages.
	if(hpid.pid==0)
	  {
	    dp = new DBFirstPage();
	    ((DBFirstPage)dp).openPage(apage);
	  }
	else
	  {
	    dp = new DBDirectoryPage();
	    ((DBDirectoryPage) dp).openPage(apage);
	  }
	nexthpid = dp.getNextPage();
	
	int entry = 0;
	
	String tmpname;
	while(entry < dp.getNumOfEntries())
	  {
	    tmpname = dp.getFileEntry(tmppid, entry);
	    
	    if((tmppid.pid != INVALID_PAGE)&&
	       (tmpname.compareTo(fname) == 0)) break; 
	    entry ++;
	  }
	
        if(entry < dp.getNumOfEntries())
	  {
	    slot = entry;
	    found = true;
	  }
	else
	  {
	    unpinPage(hpid, false /*undirty*/);
	  }
	
      } while((nexthpid.pid != INVALID_PAGE) && (!found)); // EndDO01
    
    if(!found)  // Entry not found - nothing deleted
      throw new FileEntryNotFoundException(null, "DB file not found");
    
    // Have to delete record at hpnum:slot
    tmppid.pid = INVALID_PAGE;
    dp.setFileEntry(tmppid, "\0", slot);
    
    unpinPage(hpid, true /*dirty*/);
    
  }
  
  /** Get the entry corresponding to the given file.
   *
   * @param name file entry name
   *
   * @exception IOException I/O errors
   * @exception FileIOException file I/O error
   * @exception InvalidPageNumberException invalid page number
   * @exception DiskMgrException error caused by other layers
   */
  public PageId get_file_entry(String name)
    throws IOException,
	   FileIOException,
	   InvalidPageNumberException, 
	   DiskMgrException {

    Page apage = new Page();
    boolean found = false;
    int slot = 0;
    PageId hpid = new PageId();
    PageId nexthpid = new PageId(0);
    DBHeaderPage dp;
    
    do
      {// Start DO01
	
	// System.out.println("get_file_entry do-loop01: "+name);
        hpid.pid = nexthpid.pid;
	
        // Pin the header page.
        pinPage(hpid, apage, false /*no diskIO*/);
	
	// This complication is because the first page has a different
        // structure from that of subsequent pages.
	if(hpid.pid==0)
	  {
	    dp = new DBFirstPage();
	    ((DBFirstPage) dp).openPage(apage);
	  }
	else
	  {
	    dp = new DBDirectoryPage();
	    ((DBDirectoryPage) dp).openPage(apage);
	  }
	nexthpid = dp.getNextPage();
	
	int entry = 0;
	PageId tmppid = new PageId();
	String tmpname;
	
	while(entry < dp.getNumOfEntries())
	  {
	    tmpname = dp.getFileEntry(tmppid, entry);
	    
	    if((tmppid.pid != INVALID_PAGE)&&
	       (tmpname.compareTo(name) == 0)) break; 
	    entry ++;
	  }
	if(entry < dp.getNumOfEntries())
	  {
	    slot =  entry;
	    found = true;
	  }
	
	unpinPage(hpid, false /*undirty*/);
	
      }while((nexthpid.pid!=INVALID_PAGE)&&(!found));// End of DO01
    
    if(!found)  // Entry not found - don't post error, just fail.
      {    
	//  System.out.println("entry NOT found");
	return null;
      }
    
    PageId startpid = new PageId();
    dp.getFileEntry(startpid, slot);
    return startpid;
  }
  
  /** Functions to return some characteristics of the database.
   */
  public String db_name(){return name;}
  public int db_num_pages(){return num_pages;}
  public int db_page_size(){return MINIBASE_PAGESIZE;}
  
  /** Print out the space map of the database.
   * The space map is a bitmap showing which
   * pages of the db are currently allocated.
   *
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   * @exception InvalidPageNumberException invalid page number
   * @exception DiskMgrException error caused by other layers
   */
  public void dump_space_map()
    throws DiskMgrException,
	   IOException,
	   FileIOException, 
	   InvalidPageNumberException 
	   
    {
      
      System.out.println ("********  IN DUMP");
      int num_map_pages = (num_pages + bits_per_page -1)/bits_per_page;
      int bit_number = 0;
      
      // This loop goes over each page in the space map.
      PageId pgid = new PageId();
      System.out.println ("num_map_pages = " + num_map_pages);
      System.out.println ("num_pages = " + num_pages);
      for(int i=0; i< num_map_pages; i++)
	{//start forloop01
	  
	  pgid.pid = 1 + i;   //space map starts at page1
	  // Pin the space-map page.
	  Page apage = new Page();
	  pinPage(pgid, apage, false/*read disk*/);
	  
	  // How many bits should we examine on this page?
	  int num_bits_this_page = num_pages - i*bits_per_page;
	  System.out.println ("num_bits_this_page = " + num_bits_this_page);
	  System.out.println ("num_pages = " + num_pages);
	  if ( num_bits_this_page > bits_per_page )
	    num_bits_this_page = bits_per_page;
	  
	  // Walk the page looking for a sequence of 0 bits of the appropriate
	  // length.  The outer loop steps through the page's bytes, the inner
	  // one steps through each byte's bits.
	  
	  int pgptr = 0;
	  byte [] pagebuf = apage.getpage();
	  int mask;
	  for ( ; num_bits_this_page > 0; pgptr ++)
	    {// start forloop02
	      
	      for(mask=1;
		  mask < 256 && num_bits_this_page > 0;
		  mask=(mask<<1), --num_bits_this_page, ++bit_number )
		{//start forloop03
		  
		  int bit = pagebuf[pgptr] & mask;
		  if((bit_number%10) == 0)
		    if((bit_number%50) == 0)
		      {
			if(bit_number>0) System.out.println("\n");
			System.out.print("\t" + bit_number +": ");
		      }
		    else System.out.print(' ');
		  
		  if(bit != 0) System.out.print("1");
		  else System.out.print("0");
		  
		}//end of forloop03
	      
	    }//end of forloop02
	  
	  unpinPage(pgid, false /*undirty*/);
	  
	}//end of forloop01
      
      System.out.println();
      
      
    }
  
  private RandomAccessFile fp;
  private int num_pages;
  private String name;
  
  
  /** Set runsize bits starting from start to value specified
   */
  private void set_bits( PageId start_page, int run_size, int bit )
    throws InvalidPageNumberException, 
	   FileIOException, 
	   IOException, 
	   DiskMgrException {

    if((start_page.pid<0) || (start_page.pid+run_size > num_pages))
      throw new InvalidPageNumberException(null, "Bad page number");
    
    // Locate the run within the space map.
    int first_map_page = start_page.pid/bits_per_page + 1;
    int last_map_page = (start_page.pid+run_size-1)/bits_per_page +1;
    int first_bit_no = start_page.pid % bits_per_page;
    
    // The outer loop goes over all space-map pages we need to touch.
    
    for(PageId pgid = new PageId(first_map_page);
	pgid.pid <= last_map_page;
	pgid.pid = pgid.pid+1, first_bit_no = 0)
      {//Start forloop01
	
        // Pin the space-map page.
	Page pg = new Page();
	
	
	pinPage(pgid, pg, false/*no diskIO*/);
	
	
	byte [] pgbuf = pg.getpage();
	
	// Locate the piece of the run that fits on this page.
	int first_byte_no = first_bit_no/8;
	int first_bit_offset = first_bit_no%8;
	int last_bit_no = first_bit_no + run_size -1;
	
	if(last_bit_no >= bits_per_page )
	  last_bit_no = bits_per_page - 1;
	
        int last_byte_no = last_bit_no / 8;
        
	// This loop actually flips the bits on the current page.
	int cur_posi = first_byte_no;
	for(;cur_posi <= last_byte_no; ++cur_posi, first_bit_offset=0)
	  {//start forloop02
	    
	    int max_bits_this_byte = 8 - first_bit_offset;
	    int num_bits_this_byte = (run_size > max_bits_this_byte?
				      max_bits_this_byte : run_size);
	    
            int imask =1;
	    int temp;
	    imask = ((imask << num_bits_this_byte) -1)<<first_bit_offset;
	    Integer intmask = new Integer(imask);
	    Byte mask = new Byte(intmask.byteValue());
	    byte bytemask = mask.byteValue();
	    
	    if(bit==1)
	      {
	        temp = (pgbuf[cur_posi] | bytemask);
	        intmask = new Integer(temp);
		pgbuf[cur_posi] = intmask.byteValue();
	      }
	    else
	      {
		
		temp = pgbuf[cur_posi] & (255^bytemask);
	        intmask = new Integer(temp);
		pgbuf[cur_posi] = intmask.byteValue();
	      }
	    run_size -= num_bits_this_byte;
	    
	  }//end of forloop02
	
	// Unpin the space-map page.
	
	unpinPage(pgid, true /*dirty*/);
	
      }//end of forloop01
    
  }

  /**
   * short cut to access the pinPage function in bufmgr package.
   * @see bufmgr.pinPage
   */
  private void pinPage(PageId pageno, Page page, boolean emptyPage)
    throws DiskMgrException {

    try {
      SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
    }
    catch (Exception e) {
      throw new DiskMgrException(e,"DB.java: pinPage() failed");
    }

  } // end of pinPage

  /**
   * short cut to access the unpinPage function in bufmgr package.
   * @see bufmgr.unpinPage
   */
  private void unpinPage(PageId pageno, boolean dirty)
    throws DiskMgrException {

    try {
      SystemDefs.JavabaseBM.unpinPage(pageno, dirty); 
    }
    catch (Exception e) {
      throw new DiskMgrException(e,"DB.java: unpinPage() failed");
    }

  } // end of unpinPage






////////////////////////////////////variable change required//////////////////////////////////////
    public void rdfDB(int type)
    {
        int keytype = AttrType.attrString;

        /** Initialize counter to zero **/
        PCounter.initialize();

        //Create TEMP Quadruple heap file /TOFIX
        try
        {
            TQuadrupleHF = new QuadrupleHeapfile("tempresult");
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
            QuadrupleHF = new QuadrupleHeapfile(usedbname+"/QuadrupleHF");

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
            Entity_HF = new LabelHeapfile(usedbname+"/entityHF");
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
            Predicate_HF = new LabelHeapfile(usedbname+"/predicateHF");
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
            Entity_BTree = new LabelBTreeFile(usedbname+"/entityBT",keytype,255,1);
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
            Predicate_BTree = new LabelBTreeFile(usedbname+"/predicateBT",keytype,255,1);
            Predicate_BTree.close();
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
            //System.out.println("Creating new Triple Binary Tree file");
            QuadrupleBTree = new QuadrupleBTreeFile(usedbname+"/tripleBT",keytype,255,1);
            QuadrupleBTree.close();
        }
        catch(Exception e)
        {
            System.err.println (""+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        try
        {
            //System.out.println("Creating new Label Binary Tree file for checking duplicate subjects");
            dup_tree = new LabelBTreeFile(usedbname+"/dupSubjBT",keytype,255,1);
            dup_tree.close();
        }
        catch(Exception e)
        {
            System.err.println (""+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        try
        {
            //System.out.println("Creating new Label Binary Tree file for checking duplicate objects");
            dup_Objtree = new LabelBTreeFile(usedbname+"/dupObjBT",keytype,255,1);
            dup_Objtree.close();
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
            //System.out.println("Creating Triple Binary Tree file for given index option");
            QuadrupleBTreeIndex = new QuadrupleBTreeFile(usedbname+"/Triple_BTreeIndex",keytype,255,1);
            QuadrupleBTreeIndex.close();
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
            QuadrupleHF = new QuadrupleHeapfile(usedbname+"/QuadrupleHF");
            Total_Quadruples = QuadrupleHF.getRecCnt();
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
     *  Get count of Predicates(unique) in RDF DB
     *  @return int number of distinct Predicates
     */
    public int getPredicateCnt()
    {
        try
        {
            Predicate_HF = new LabelHeapfile(usedbname+"/predicateHF");
            Total_Predicates = Predicate_HF.getRecCnt();
        }
        catch (Exception e)
        {
            System.err.println (""+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return Total_Predicates;
    }

    /**
     *  Get count of Subjects(unique) in RDF DB
     *  @return int number of distinct subjects
     */
    public int getSubjectCnt()
    {
        Total_Subjects = 0;
        KeyDataEntry entry = null;
        KeyDataEntry dup_entry = null;
        try
        {
            QuadrupleBTree = new QuadrupleBTreeFile(curr_dbname+"/tripleBT");
            int keytype = AttrType.attrString;
            dup_tree = new LabelBTreeFile(usedbname+"/dupSubjBT");
            //Start Scanning Btree to check if  predicate already present
            QuadrupleBTFileScan scan = QuadrupleBTree.new_scan(null,null);
            do
            {
                entry = scan.get_next();
                if(entry != null)
                {
                    String label = ((StringKey)(entry.key)).getKey();
                    String[] temp;
                    /* delimiter */
                    String delimiter = ":";
                    /* given string will be split by the argument delimiter provided. */
                    temp = label.split(delimiter);
                    String subject = temp[0] + temp[1];
                    //Start Scaning Label Btree to check if subject already present
                    KeyClass low_key = new StringKey(subject);
                    KeyClass high_key = new StringKey(subject);
                    LabelBTFileScan dup_scan = dup_tree.new_scan(low_key,high_key);
                    dup_entry = dup_scan.get_next();
                    if(dup_entry == null)
                    {
                        //subject not present in btree, hence insert
                        dup_tree.insert(low_key,new LID(new PageId(Integer.parseInt(temp[1])),Integer.parseInt(temp[0])));

                    }
                    dup_scan.DestroyBTreeFileScan();

                }

            }while(entry!=null);
            scan.DestroyBTreeFileScan();
            QuadrupleBTree.close();

            KeyClass low_key = null;
            KeyClass high_key = null;
            LabelBTFileScan dup_scan = dup_tree.new_scan(low_key,high_key);
            do
            {
                dup_entry = dup_scan.get_next();
                if(dup_entry!=null)
                    Total_Subjects++;

            }while(dup_entry!=null);
            dup_scan.DestroyBTreeFileScan();
            dup_tree.close();
        }
        catch(Exception e)
        {
            System.err.println (""+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return Total_Subjects;
    }

    /**
     *  Get count of Entities(unique) in RDF DB
     *  @return int number of distinct Entities
     */
    public int getEntityCnt()
    {
        try
        {
            Entity_HF = new LabelHeapfile(usedbname+"/entityHF");
            Total_Entities = Entity_HF.getRecCnt();
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
     *  Get count of Objects(unique) in RDF DB
     *  @return int number of distinct objects
     */
    public int getObjectCnt()
    {
        Total_Objects = 0;
        KeyDataEntry entry = null;
        KeyDataEntry dup_entry = null;
        try
        {
            QuadrupleBTree = new QuadrupleBTreeFile(usedbname+"/tripleBT");
            int keytype = AttrType.attrString;
            dup_Objtree = new LabelBTreeFile(usedbname+"/dupObjBT");
            //Start Scaning Btree to check if  predicate already present
            QuadrupleBTFileScan scan = QuadrupleBTree.new_scan(null,null);
            do
            {
                entry = scan.get_next();
                if(entry != null)
                {
                    String label = ((StringKey)(entry.key)).getKey();
                    String[] temp;
                    /* delimiter */
                    String delimiter = ":";
                    /* given string will be split by the argument delimiter provided. */
                    temp = label.split(delimiter);
                    String object = temp[4] + temp[5];
                    //Start Scaning Label Btree to check if subject already present
                    KeyClass low_key = new StringKey(object);
                    KeyClass high_key = new StringKey(object);
                    LabelBTFileScan dup_scan = dup_Objtree.new_scan(low_key,high_key);
                    dup_entry = dup_scan.get_next();
                    if(dup_entry == null)
                    {
                        //subject not present in btree, hence insert
                        dup_Objtree.insert(low_key,new LID(new PageId(Integer.parseInt(temp[4])),Integer.parseInt(temp[5])));

                    }
                    dup_scan.DestroyBTreeFileScan();

                }

            }while(entry!=null);
            scan.DestroyBTreeFileScan();
            QuadrupleBTree.close();

            KeyClass low_key = null;
            KeyClass high_key = null;
            LabelBTFileScan dup_scan = dup_Objtree.new_scan(low_key,high_key);
            do
            {
                dup_entry = dup_scan.get_next();
                if(dup_entry!=null)
                    Total_Objects++;

            }while(dup_entry!=null);
            dup_scan.DestroyBTreeFileScan();
            dup_Objtree.close();
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
            Entity_BTree = new LabelBTreeFile(usedbname+"/entityBT");
            //      LabelBT.printAllLeafPages(Entity_BTree.getHeaderPage());

            LID lid = null;
            KeyClass low_key = new StringKey(EntityLabel);
            KeyClass high_key = new StringKey(EntityLabel);
            KeyDataEntry entry = null;

            //Start Scaning Btree to check if entity already present
            LabelBTFileScan scan = Entity_BTree.new_scan(low_key,high_key);
            entry = scan.get_next();
            if(entry!=null)
            {
                if(EntityLabel.equals(((StringKey)(entry.key)).getKey()))
                {
                    //return already existing EID ( convert lid to EID)
                    lid =  ((LabelLeafData)entry.data).getData();
                    entityid = lid.returnEID();
                    scan.DestroyBTreeFileScan();
                    Entity_BTree.close();
                    return entityid;
                }
            }

            scan.DestroyBTreeFileScan();
            //Insert into Entity HeapFile
            lid = Entity_HF.insertRecord(EntityLabel.getBytes());

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
            Entity_HF = new LabelHeapfile(usedbname+"/entityHF");
            Entity_BTree = new LabelBTreeFile(usedbname+"/entityBT");
            //      LabelBT.printAllLeafPages(Entity_BTree.getHeaderPage());

            LID lid = null;
            KeyClass low_key = new StringKey(EntityLabel);
            KeyClass high_key = new StringKey(EntityLabel);
            KeyDataEntry entry = null;

            //Start Scaning Btree to check if entity already present
            LabelBTFileScan scan = Entity_BTree.new_scan(low_key,high_key);
            entry = scan.get_next();
            if(entry!=null)
            {
                if(EntityLabel.equals(((StringKey)(entry.key)).getKey()))
                {
                    //System.out.println(((StringKey)(entry.key)).getKey());
                    lid =  ((LabelLeafData)entry.data).getData();
                    success = Entity_HF.deleteRecord(lid) & Entity_BTree.Delete(low_key,lid);
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
            Predicate_BTree = new LabelBTreeFile(usedbname+"/predicateBT");
            //LabelBT.printAllLeafPages(Predicate_BTree.getHeaderPage());
            KeyClass low_key = new StringKey(PredicateLabel);
            KeyClass high_key = new StringKey(PredicateLabel);
            KeyDataEntry entry = null;

            //Start Scaning Btree to check if  predicate already present
            LabelBTFileScan scan = Predicate_BTree.new_scan(low_key,high_key);
            entry = scan.get_next();
            if(entry != null)
            {
                if(PredicateLabel.compareTo(((StringKey)(entry.key)).getKey()) == 0)
                {
                    //return already existing EID ( convert lid to EID)
                    predicateid = ((LabelLeafData)(entry.data)).getData().returnPID();
                    scan.DestroyBTreeFileScan();
                    Predicate_BTree.close(); //Close the Predicate Btree file
                    return predicateid;
                }
            }
            scan.DestroyBTreeFileScan();
            //Insert into Predicate HeapFile
            lid = Predicate_HF.insertRecord(PredicateLabel.getBytes());
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
            Predicate_HF = new LabelHeapfile(usedbname+"/predicateHF");
            Predicate_BTree = new LabelBTreeFile(usedbname+"/predicateBT");
            //      LabelBT.printAllLeafPages(Entity_BTree.getHeaderPage());

            LID lid = null;
            KeyClass low_key = new StringKey(PredicateLabel);
            KeyClass high_key = new StringKey(PredicateLabel);
            KeyDataEntry entry = null;

            //Start Scanning BTree to check if entity already present
            LabelBTFileScan scan = Predicate_BTree.new_scan(low_key,high_key);
            entry = scan.get_next();
            if(entry!=null)
            {
                if(PredicateLabel.equals(((StringKey)(entry.key)).getKey()))
                {
                    //System.out.println(((StringKey)(entry.key)).getKey());
                    lid =  ((LabelLeafData)entry.data).getData();
                    success = Predicate_HF.deleteRecord(lid) & Predicate_BTree.Delete(low_key,lid);
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
    public QID insertQuadruple(byte[] QuadruplePtr)
            throws Exception
    {
        QID Quadrupleid;
        QID qid = null;
        try
        {
            //Open Triple BTree Index file
            QuadrupleBTree = new QuadrupleBTreeFile(usedbname+"/QuadrupleBT");
            //TripleBT.printAllLeafPages(Triple_BTree.getHeaderPage());
            int sub_slotNo = Convert.getIntValue(0,triplePtr);
            int sub_pageNo = Convert.getIntValue(4,triplePtr);
            int pred_slotNo = Convert.getIntValue(8,triplePtr);
            int pred_pageNo = Convert.getIntValue(12,triplePtr);
            int obj_slotNo = Convert.getIntValue(16,triplePtr);
            int obj_pageNo = Convert.getIntValue(20,triplePtr);
            double confidence =Convert.getDoubleValue(24,triplePtr);
            String key = new String(Integer.toString(sub_slotNo) +':'+ Integer.toString(sub_pageNo) +':'+ Integer.toString(pred_slotNo) + ':' + Integer.toString(pred_pageNo) +':' + Integer.toString(obj_slotNo) +':'+ Integer.toString(obj_pageNo));
            KeyClass low_key = new StringKey(key);
            KeyClass high_key = new StringKey(key);
            KeyDataEntry entry = null;

            //Start Scaning Btree to check if  predicate already present
            QuadrupleBTFileScan scan = QuadrupleBTree.new_scan(low_key,high_key);
            entry = scan.get_next();
            if(entry != null)
            {
                //System.out.println("Duplicate Triple found : " + ((StringKey)(entry.key)).getKey());
                if(key.compareTo(((StringKey)(entry.key)).getKey()) == 0)
                {
                    //return already existing TID
                    Quadrupleid = ((QuadrupleLeafData)(entry.data)).getData();
                    Quadruple record = Triple_HF.getRecord(tripleid);
                    double orig_confidence = record.getConfidence();
                    if(orig_confidence > confidence)
                    {
                        Quadruple newRecord = new Quadruple(triplePtr,0,32);
                        Triple_HF.updateRecord(tripleid,newRecord);
                    }
                    scan.DestroyBTreeFileScan();
                    Triple_BTree.close();
                    return tripleid;
                }
            }

            //insert into triple heap file
            //System.out.println("("+triplePtr+")");
            qid= Triple_HF.insertTriple(triplePtr);

            //System.out.println("Inserting triple key : "+ key + "tid : " + tid);
            //insert into triple btree
            QuadrupleBTree.insert(low_key,qid);

            scan.DestroyBTreeFileScan();
            QuadrupleBTree.close();
        }
        catch(Exception e)
        {
            System.err.println ("*** Error inserting Quadruple record " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        return qid;
    }

    public boolean deleteQuadruple(byte[] QuadruplePtr)
    {
        boolean success = false;
        QID quadrupleid = null;
        try
        {
            //Open Triple BTree Index file
            QuadrupleBTree = new QuadrupleBTreeFile(usedbname+"/QuadrupleBT");
            //TripleBT.printAllLeafPages(Triple_BTree.getHeaderPage());
            int sub_slotNo = Convert.getIntValue(0,triplePtr);
            int sub_pageNo = Convert.getIntValue(4,triplePtr);
            int pred_slotNo = Convert.getIntValue(8,triplePtr);
            int pred_pageNo = Convert.getIntValue(12,triplePtr);
            int obj_slotNo = Convert.getIntValue(16,triplePtr);
            int obj_pageNo = Convert.getIntValue(20,triplePtr);
            double confidence =Convert.getDoubleValue(24,triplePtr);
            String key = new String(Integer.toString(sub_slotNo) +':'+ Integer.toString(sub_pageNo) +':'+ Integer.toString(pred_slotNo) + ':' + Integer.toString(pred_pageNo) +':' + Integer.toString(obj_slotNo) +':'+ Integer.toString(obj_pageNo));
            //System.out.println(key);
            KeyClass low_key = new StringKey(key);
            KeyClass high_key = new StringKey(key);
            KeyDataEntry entry = null;

            //Start Scaning Btree to check if  predicate already present
            QuadrupleBTFileScan scan = QuadrupleBTree.new_scan(low_key,high_key);
            entry = scan.get_next();
            if(entry != null)
            {
                //System.out.println("Triple found : " + ((StringKey)(entry.key)).getKey());
                if(key.compareTo(((StringKey)(entry.key)).getKey()) == 0)
                {
                    //return already existing TID
                    quadrupleid = ((QuadrupleLeafData)(entry.data)).getData();
                    if(quadrupleid!=null)
                        success = Triple_HF.deleteRecord(quadrupleid);
                }
            }
            scan.DestroyBTreeFileScan();
            if(entry!=null)
            {
                if(low_key!=null && quadrupleid!=null)
                    success = success & QuadrupleBTree.Delete(low_key,quadrupleid);
            }

            QuadrupleBTree.close();

        }
        catch(Exception e)
        {
            System.err.println ("*** Error deleting Quadruple record " + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        return success;
    }
}//end of DB class

