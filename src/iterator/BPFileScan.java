package iterator;


import bufmgr.PageNotReadException;
import global.*;
import heap.*;

import java.io.IOException;

/**
 *open a heapfile and according to the condition expression to get
 *output file, call get_next to get all tuples
 */
public abstract class BPFileScan extends BPIterator
{
  private AttrType[] _in1;
  private short in1_len;
  private short[] s_sizes;
  private Heapfile f;
  private Scan scan;
  private Tuple     tuple1;
  private Tuple    Jtuple;
  private int        t1_size;
  private int nOutFlds;
  private CondExpr[]  OutputFilter;
  public FldSpec[] perm_mat;
  public int totalNumberOfAttributes;



  /**
   *constructor
   *@param file_name heapfile to be opened
   *@param in1[]  array showing what the attributes of the input fields are.
   *@param s1_sizes[]  shows the length of the string fields.
   *@param len_in1  number of attributes in the input tuple
   *@param n_out_flds  number of fields in the out tuple
   *@param proj_list  shows what input fields go where in the output tuple
   *@param outFilter  select expressions
   *@exception IOException some I/O fault
   *@exception FileScanException exception from this class
   *@exception TupleUtilsException exception from this class
   *@exception InvalidRelation invalid relation
   */
  public  BPFileScan (String  file_name, int n_out_flds)
          throws IOException,
          FileScanException,
          TupleUtilsException,
          InvalidRelation
  {
      tuple1 = new Tuple();
      nOutFlds = n_out_flds;

      totalNumberOfAttributes = nOutFlds * 2 - 1;


      int i = 0;
      for (i = 0; i < (nOutFlds - 1) * 2; i++) {
          _in1[i] = new AttrType(AttrType.attrInteger);
      }
      _in1[i] = new AttrType(AttrType.attrReal);
      s_sizes = new short[1];
      s_sizes[0] = (short) ((nOutFlds - 1) * 2 * 4 + 1 * 8);
      try {
          tuple1.setHdr((short) (totalNumberOfAttributes), _in1, s_sizes);
      } catch (InvalidTypeException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
      } catch (InvalidTupleSizeException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
      }

      try {
          scan = f.openScan();
      } catch (Exception e) {
          throw new FileScanException(e, "openScan() failed");
      }
  }
  
  /**
   *@return shows what input fields go where in the output tuple
   */
  public FldSpec[] show()
    {
      return perm_mat;
    }
  
  /**
   *@return the result tuple
   *@exception JoinsException some join exception
   *@exception IOException I/O errors
   *@exception InvalidTupleSizeException invalid tuple size
   *@exception InvalidTypeException tuple type not valid
   *@exception PageNotReadException exception from lower layer
   *@exception PredEvalException exception from PredEval class
   *@exception UnknowAttrType attribute type unknown
   *@exception FieldNumberOutOfBoundException array out of bounds
   *@exception WrongPermat exception for wrong FldSpec argument
   */
  public BasicPattern get_next()
    throws JoinsException,
	   IOException,
	   InvalidTupleSizeException,
	   InvalidTypeException,
	   PageNotReadException, 
	   PredEvalException,
	   UnknowAttrType,
	   FieldNumberOutOfBoundException,
	   WrongPermat
    {     
      RID rid = new RID();;
      
      while(true) {
	if((tuple1 =  scan.getNext(rid)) == null) {
	  return null;
	}
	
	tuple1.setHdr((short) (totalNumberOfAttributes), _in1, s_sizes);
          short len = (tuple1.noOfFlds());

          BasicPattern bp = new BasicPattern();
          int a = 0;
          int b = 0;
          for (a = 0, b = 1; a < (len / 2); a++) {
              int slot_no = tuple1.getIntFld(b++);
              int page_no = tuple1.getIntFld(b++);

              NID nid = new NID(new PageId(page_no), slot_no);
              bp.setIntFld(a + 1, nid);

          }
          double setConfidence = tuple1.getFloFld(b);
          bp.setConfidence(setConfidence);
          return bp;
      }
    }

  /**
   *implement the abstract method close() from super class Iterator
   *to finish cleaning up
   */
  public void close() 
    {
     
      if (!closeFlag) {
	scan.closescan();
	closeFlag = true;
      } 
    }
  
}


