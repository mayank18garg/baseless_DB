/* File Tuple.java */

package labelheap;

import java.io.*;
import java.lang.*;
import global.*;


public class Label implements GlobalConst{


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
  private int label_offset;

  /**
   * length of this tuple
   */
  private int label_length;

  /** 
   * private field
   * Number of fields in this tuple
   */
  private short fldCnt=1;

  /** 
   * private field
   * Array of offsets of the fields
   */
 
  private short [] fldOffset = new short[] {0}; 

   /**
    * Class constructor
    * Creat a new tuple with length = max_size,tuple offset = 0.
    */

  public  Label()
  {
       // Creat a new tuple
       data = new byte[max_size];
       label_offset = 0;
       label_length = max_size;
  }
   
   /** Constructor
    * @param alabel a byte array which contains the tuple
    * @param offset the offset of the tuple in the byte array
    * @param length the length of the tuple
    */

   public Label(byte [] alabel, int offset, int length)
   {
      data = alabel;
      label_offset = offset;
      label_length = length;
    //  fldCnt = getShortValue(offset, data);
   }
   
   /** Constructor(used as tuple copy)
    * @param fromLabel   a byte array which contains the tuple
    * 
    */
   public Label(Label fromLabel)
   {
       data = fromLabel.getLabelByteArray();
       label_length = fromLabel.getLength();
       label_offset = 0;
       fldCnt = fromLabel.noOfFlds(); 
       fldOffset = fromLabel.copyFldOffset(); 
   }

   /**  
    * Class constructor
    * Creat a new tuple with length = size,tuple offset = 0.
    */
 
  public Label(int size)
  {
       // Creat a new tuple
       data = new byte[size];
       label_offset = 0;
       label_length = size;     
  }
   
   /** Copy a tuple to the current tuple position
    *  you must make sure the tuple lengths must be equal
    * @param fromLabel the tuple being copied
    */
   public void labelCopy(Label fromLabel)
   {
       byte [] temparray = fromLabel.getLabelByteArray();
       System.arraycopy(temparray, 0, data, label_offset, label_length);   
//       fldCnt = fromTuple.noOfFlds(); 
//       fldOffset = fromTuple.copyFldOffset(); 
   }

   /** This is used when you don't want to use the constructor
    * @param alabel  a byte array which contains the tuple
    * @param offset the offset of the tuple in the byte array
    * @param length the length of the tuple
    */

   public void labelInit(byte [] alabel, int offset, int length)
   {
      data = alabel;
      label_offset = offset;
      label_length = length;
   }

 /**
  * Set a tuple with the given tuple length and offset
  * @param	record	a byte array contains the tuple
  * @param	offset  the offset of the tuple ( =0 by default)
  * @param	length	the length of the tuple
  */
 public void labelSet(byte [] record, int offset, int length)  
  {
      System.arraycopy(record, offset, data, 0, length);
      label_offset = 0;
      label_length = length;
  }
  
 /** get the length of a tuple, call this method if you did not 
  *  call setHdr () before
  * @return 	length of this tuple in bytes
  */   
  public int getLength()
   {
      return label_length;
   }

/** get the length of a tuple, call this method if you did 
  *  call setHdr () before
  * @return     size of this tuple in bytes
  */
  public short size()
   {
      return ((short) (fldOffset[fldCnt] - label_offset));
   }
 
   /** get the offset of a tuple
    *  @return offset of the tuple in byte array
    */   
   public int getOffset()
   {
      return label_offset;
   }   
   
   /** Copy the tuple byte array out
    *  @return  byte[], a byte array contains the tuple
    *		the length of byte[] = length of the tuple
    */
    
   public byte [] getLabelByteArray() 
   {
       byte [] labelcopy = new byte [label_length];
       System.arraycopy(data, label_offset, labelcopy, 0, label_length);
       return labelcopy;
   }
   
   /** return the data byte array 
    *  @return  data byte array 		
    */
    
   public byte [] returnLabelByteArray()
   {
       return data;
   }
   

  public String getLabel() throws IOException
  {
    String val = Convert.getStrValue(fldOffset[0], data, label_length);
    return val;
  }

  public Label setLabel(String label) throws IOException
  {
    Convert.setStrValue(label, fldOffset[0], data);
    return this;
  }
       
  /**
   * Returns number of fields in this tuple
   *
   * @return the number of fields in this tuple
   *
   */

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
 * @throws IOException
  * @Exception IOException I/O exception
  */
//  public void print(AttrType type[])
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
public void print() throws IOException{
  String sval;
  sval = Convert.getStrValue(fldOffset[0], data,label_length);
  System.out.print(sval);
}

  /**
   * private method
   * Padding must be used when storing different types.
   * 
   * @param	offset
   * @param type   the type of tuple
   * @return short typle
   */

  private short pad(short offset, AttrType type)
   {
      return 0;
   }
}

