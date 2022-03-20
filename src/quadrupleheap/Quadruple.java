/* File Tuple.java */

package quadrupleheap;

import java.io.*;
import java.lang.*;
import global.*;
import heap.Tuple;


public class Quadruple implements GlobalConst{


//  /** 
//   * Maximum size of any tuple
//   */
//   public static final int max_size = MINIBASE_PAGESIZE;

 /** 
   * a byte array to hold data
   */
  private byte [] data;

  /**
   * start position of this tuple in data[]
   */
  private int quadruple_offset;

  /**
   * length of this tuple
   */
  private int quadruple_length=28;

  /** 
   * private field
   * Number of fields in this tuple
   */
  private short fldCnt = 4;

  /** 
   * private field
   * Array of offsets of the fields
   */
 
  private short [] fldOffset = new short[] {0,8,16,24,28};

   /**
    * Class constructor
    * Creat a new tuple with length = max_size,tuple offset = 0.
    */

  public  Quadruple()
  {
       // Creat a new tuple
       data = new byte[quadruple_length];
       quadruple_offset = 0;
      //  quadruple_length = quad;
  }
   
   /** Constructor
    * @param atuple a byte array which contains the tuple
    * @param offset the offset of the tuple in the byte array
    * @param length the length of the tuple
    */

  public Quadruple(byte [] aquadruple, int offset)
  {
    data = aquadruple;
    quadruple_offset = offset;
    quadruple_length = 28;
  //  fldCnt = getShortValue(offset, data);
  }
   
   /** Constructor(used as tuple copy)
    * @param fromTuple   a byte array which contains the tuple
    * 
    */
  public Quadruple(Quadruple fromQuadruple)
  {
    data = fromQuadruple.getQuadrupleByteArray();
    quadruple_length = fromQuadruple.getLength();
    quadruple_offset = 0;
    fldCnt = fromQuadruple.noOfFlds(); 
    fldOffset = fromQuadruple.copyFldOffset(); 
  }

  public EID getSubjecqid() throws IOException
  {
    int slotno = Convert.getIntValue(fldOffset[0], data);
    int pageno = Convert.getIntValue(fldOffset[0]+4,data);
    PageId pageid = new PageId(pageno);
    LID subjectlid = new LID(pageid,slotno);
    return subjectlid.returnEID();
  }

  public PID getPredicateID() throws IOException
  {
    int slotno = Convert.getIntValue(fldOffset[1], data);
    int pageno = Convert.getIntValue(fldOffset[1]+4, data);
    PageId pageid = new PageId(pageno);
    LID predicatelid = new LID(pageid,slotno);
    return predicatelid.returnPID();
  }

  public EID getObjecqid() throws IOException
  {
    int slotno = Convert.getIntValue(fldOffset[2], data);
    int pageno = Convert.getIntValue(fldOffset[2]+4,data);
    PageId pageid = new PageId(pageno);
    LID objectlid = new LID(pageid,slotno);
    return objectlid.returnEID();
  }

  public double getConfidence() throws IOException
  {
    double confidence = Convert.getFloValue(fldOffset[3], data);
    return confidence;
  }

  public int getLength()
  {
    return quadruple_length;
  }

  public Quadruple setSubjecqid (EID subjecqid) throws IOException
  {
    Convert.setIntValue (subjecqid.slotNo, fldOffset[0], data);
    Convert.setIntValue (subjecqid.pageNo.pid, fldOffset[0]+4, data);

	  return this;
  }

  public Quadruple setPredicateID(PID predicateID) throws IOException
  {
    Convert.setIntValue(predicateID.slotNo, fldOffset[1], data);
    Convert.setIntValue(predicateID.pageNo.pid, fldOffset[1]+4, data);
    return this;
  }

  public Quadruple setobjecqid(EID objecqid) throws IOException
  {
    Convert.setIntValue(objecqid.slotNo, fldOffset[2], data);
    Convert.setIntValue(objecqid.pageNo.pid, fldOffset[2]+4, data);
    return this;
  }

  public Quadruple setConfidence(double confidence) throws IOException
  {
    Convert.setFloValue((float) confidence, fldOffset[3], data);
    return this;
  }

   /**  
    * Class constructor
    * Creat a new tuple with length = size,tuple offset = 0.
    */
 
  // public  Quadruple(int size)
  // {
  //      // Creat a new tuple
  //      data = new byte[size];
  //      quadruple_offset = 0;
  //      quadruple_length = size;     
  // }
   
   /** Copy a tuple to the current tuple position
    *  you must make sure the tuple lengths must be equal
    * @param fromTuple the tuple being copied
    */
  public void quadrupleCopy(Quadruple fromQuadruple)
  {
    byte [] temparray = fromQuadruple.getQuadrupleByteArray();
    System.arraycopy(temparray, 0, data, quadruple_offset, quadruple_length);   
//       fldCnt = fromTuple.noOfFlds(); 
//       fldOffset = fromTuple.copyFldOffset(); 
  }

   /** This is used when you don't want to use the constructor
    * @param atuple  a byte array which contains the tuple
    * @param offset the offset of the tuple in the byte array
    * @param length the length of the tuple
    */

   public void quadrupleInit(byte [] aquadruple, int offset)
   {
      data = aquadruple;
      quadruple_offset = offset;
      // quadruple_length = length;
   }

 /**
  * Set a tuple with the given tuple length and offset
  * @param	record	a byte array contains the tuple
  * @param	offset  the offset of the tuple ( =0 by default)
  * @param	length	the length of the tuple
  */
 public void quadrupleSet(byte [] aquadruple, int offset)  
  {
      System.arraycopy(aquadruple, offset, data, 0, quadruple_length);
      quadruple_offset = 0;
      // tuple_length = length;
  }
  
 
/** get the length of a tuple, call this method if you did 
  *  call setHdr () before
  * @return     size of this tuple in bytes
  */
  public short size()
   {
      return ((short) (fldOffset[fldCnt] - quadruple_offset));
   }
 
   /** get the offset of a tuple
    *  @return offset of the tuple in byte array
    */   
    public int getOffset()
    {
       return quadruple_offset;
    }
   
   /** Copy the tuple byte array out
    *  @return  byte[], a byte array contains the tuple
    *		the length of byte[] = length of the tuple
    */
    
   public byte [] getQuadrupleByteArray() 
   {
       byte [] quadruplecopy = new byte [quadruple_length];
       System.arraycopy(data, quadruple_offset, quadruplecopy, 0, quadruple_length);
       return quadruplecopy;
   }
   
   /** return the data byte array 
    *  @return  data byte array 		
    */
    
    public byte [] returnQuadrupleByteArray()
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
  * @param type  the types in the tuple
  * @Exception IOException I/O exception
  */
 public void print()
         throws IOException
 {
     int i, val;
     float fval;
     // String sval;

     System.out.print("[");
     for (i=0; i< fldCnt-1; i++)
     {
         val = Convert.getIntValue(fldOffset[i], data);
         System.out.print(val);
         System.out.print(", ");
     }

     fval = Convert.getFloValue(fldOffset[i], data);
     System.out.print(fval);
     System.out.println("]");
 }
// public void print(AttrType type[])
//     throws IOException 
//  {
//   int i, val;
//   float fval;
//   String sval;

//   System.out.print("[");
//   for (i=0; i< fldCnt-1; i++)
//    {
//     switch(type[i].attrType) {

//    case AttrType.attrInteger:
//      val = Convert.getIntValue(fldOffset[i], data);
//      System.out.print(val);
//      break;

//    case AttrType.attrReal:
//      fval = Convert.getFloValue(fldOffset[i], data);
//      System.out.print(fval);
//      break;

//    case AttrType.attrString:
//      sval = Convert.getStrValue(fldOffset[i], data,fldOffset[i+1] - fldOffset[i]);
//      System.out.print(sval);
//      break;
  
//    case AttrType.attrNull:
//    case AttrType.attrSymbol:
//      break;
//    }
//    System.out.print(", ");
//  } 
 
//  switch(type[fldCnt-1].attrType) {

//    case AttrType.attrInteger:
//      val = Convert.getIntValue(fldOffset[i], data);
//      System.out.print(val);
//      break;

//    case AttrType.attrReal:
//      fval = Convert.getFloValue(fldOffset[i], data);
//      System.out.print(fval);
//      break;

//    case AttrType.attrString:
//      sval = Convert.getStrValue(fldOffset[i], data,fldOffset[i+1] - fldOffset[i]);
//      System.out.print(sval);
//      break;

//    case AttrType.attrNull:
//    case AttrType.attrSymbol:
//      break;
//    }
//    System.out.println("]");

//  }

  
}

