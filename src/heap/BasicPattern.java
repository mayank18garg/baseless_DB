/* File Tuple.java */

package heap;

import global.*;

import java.io.IOException;


public class BasicPattern implements GlobalConst{


  /**
   * Maximum size of any tuple
   */
   public static final int max_size = MINIBASE_PAGESIZE;

 /**
   * a byte array to hold data
   */
  private byte [] data;

  /**
   * start position of this tuple in data[]
   */
  private int bp_offset;

  /**
   * length of this tuple
   */
  private int bp_length;

  /**
   * private field
   * Number of fields in this tuple
   */
  private short fldCnt;

  /**
   * private field
   * Array of offsets of the fields
   */

  private short [] fldOffset;

    /**
     * private field
     * default confidence
     */

    private double confidence;

   /**
    * Class constructor
    * Creat a new tuple with length = max_size,tuple offset = 0.
    */

  public BasicPattern()
  {
       // Creat a new tuple
       data = new byte[max_size];
       bp_offset = 0;
       bp_length = max_size;
  }

   /** Constructor
    * @param abp a byte array which contains the tuple
    * @param offset the offset of the tuple in the byte array
    * @param length the length of the tuple
    */

  public BasicPattern(byte [] abp, int offset, int length)
  {
    data = abp;
    bp_offset = offset;
    bp_length = length;
    fldCnt = (short) (length/4); //length is the byte array size, 4 is the number of bytes required to store an integer
    setFldOffset(fldCnt/2, offset);
  }

    private void setFldOffset(int numOfNodes, int offset) {

      fldOffset[0] = (short)(offset);
      for(int i = 1;i<numOfNodes;i++){
          fldOffset[i] = (short) (fldOffset[i-1]+8);
      }
    }

    /** Constructor(used as tuple copy)
    * @param fromBasicPattern   a byte array which contains the tuple
    *
    */
  public BasicPattern(BasicPattern fromBasicPattern)
  {
    data = fromBasicPattern.getBPByteArray();
    bp_length = fromBasicPattern.getLength();
    bp_offset = 0;
    fldCnt = fromBasicPattern.noOfFlds();
    fldOffset = fromBasicPattern.copyFldOffset();
  }

  public NID getNodeID(int fldNo )
          throws IOException, FieldNumberOutOfBoundException {
      if ( (fldNo > 0) && (fldNo < fldCnt))
      {
          int slotno = Convert.getIntValue(fldOffset[fldNo -1], data);
          int pageno = Convert.getIntValue(fldOffset[fldNo -1]+4,data);
          PageId pageid = new PageId(pageno);
          NID nodeid = new NID(pageid,slotno);
          return nodeid;
      }
      else
          throw new FieldNumberOutOfBoundException (null, "BASICPATTERN:TUPLE_FLDNO_OUT_OF_BOUND");
  }

  public double getConfidence()
  {
    return confidence;
  }

  public int getLength()
  {
    return bp_length;
  }

    public BasicPattern setIntFld(int fldNo, NID nid)
            throws IOException, FieldNumberOutOfBoundException
    {
        if ( (fldNo > 0) && (fldNo < fldCnt))
        {
            Convert.setIntValue (nid.slotNo, fldOffset[fldNo -1], data);
            Convert.setIntValue (nid.pageNo.pid, fldOffset[fldNo -1]+4, data);
            return this;
        }
        else
            throw new FieldNumberOutOfBoundException (null, "BASICPATTERN:TUPLE_FLDNO_OUT_OF_BOUND");
    }

  public BasicPattern setConfidence(double confidence) throws IOException
  {
    this.confidence = confidence;
    return this;
  }

   /** Copy a tuple to the current tuple position
    *  you must make sure the tuple lengths must be equal
    * @param fromBasicPattern the tuple being copied
    */
  public void basicPatternCopy(BasicPattern fromBasicPattern)
  {
    byte [] temparray = fromBasicPattern.getBPByteArray();
    System.arraycopy(temparray, 0, data, bp_offset, bp_length);
    fldCnt = fromBasicPattern.noOfFlds();
    fldOffset = fromBasicPattern.copyFldOffset();
  }

   /** This is used when you don't want to use the constructor
    * @param abp  a byte array which contains the tuple
    * @param offset the offset of the tuple in the byte array
    * @param length the length of the tuple
    */

   public void basicPatternInit(byte [] abp, int offset, int length)
   {
       data = abp;
       bp_offset = offset;
       bp_length = length;
       fldCnt = (short) (length/4); //length is the byte array size, 4 is the number of bytes required to store an integer
       setFldOffset(fldCnt/2, offset);
   }

 /**
  * Set a tuple with the given tuple length and offset
  * @param	record	a byte array contains the tuple
  * @param	offset  the offset of the tuple ( =0 by default)
  * @param	length	the length of the tuple
  */
 public void basicPatternSet(byte [] record, int offset, int length)
 {
     System.arraycopy(record, offset, data, 0, length);
     bp_offset = 0;
     bp_length = length;
     fldCnt = (short) (length/4); //length is the byte array size, 4 is the number of bytes required to store an integer
     setFldOffset(fldCnt/2, offset);
 }


/** get the length of a tuple, call this method if you did
  *  call setHdr () before
  * @return     size of this tuple in bytes
  */
  public short size()
   {
      return ((short) (fldOffset[fldCnt] - bp_offset));
   }
 
   /** get the offset of a tuple
    *  @return offset of the tuple in byte array
    */   
    public int getOffset()
    {
       return bp_offset;
    }
   
   /** Copy the tuple byte array out
    *  @return  byte[], a byte array contains the tuple
    *		the length of byte[] = length of the tuple
    */
    
   public byte [] getBPByteArray()
   {
       byte [] bpcopy = new byte [bp_length];
       System.arraycopy(data, bp_offset, bpcopy, 0, bp_length);
       return bpcopy;
   }
   
   /** return the data byte array 
    *  @return  data byte array 		
    */
    
    public byte [] returnBPByteArray()
    {
        return data;
    }
   
  

  public short noOfFlds()
   {
     return fldCnt;
   }

  /**
   * Makes a copy of the fldOffset array
   *
   * @return a copy of the fldOffset arrray
   *
   */

  public short[] copyFldOffset() 
   {
     short[] newFldOffset = new short[fldCnt + 1];
     for (int i=0; i<=fldCnt; i++) {
       newFldOffset[i] = fldOffset[i];
     }
     
     return newFldOffset;
   }


    /**
  * Print out the tuple
  * @Exception IOException I/O exception
  */
  public void print()
  throws IOException 
{
int i, val1, val2;
float fval;
// String sval;

System.out.print("[");
for (i=0; i< fldCnt; i++)
{ 
  val1 = Convert.getIntValue(fldOffset[i], data);
  val2 = Convert.getIntValue(fldOffset[i]+4, data);
  System.out.print(val1+"_"+val2);
  System.out.print(", ");
}
  //print confidence
  System.out.print(confidence);
  System.out.println("]");
}


  
}

