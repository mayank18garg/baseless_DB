//package labelbtree;
package btree;

import global.LID;
//import btree.*;

/**  IndexData: It extends the DataClass.
 *   It defines the data "lid" for leaf node in B++ tree.
 */
public class LabelHeapLeafData extends DataClass {
  private LID myLid;

  public String toString() {
     String s;
     s="[ "+ (new Integer(myLid.pageNo.pid)).toString() +" "
              + (new Integer(myLid.slotNo)).toString() + " ]";
     return s;
  }

  /** Class constructor
   *  @param    lid  the data lid
   */
  public LabelHeapLeafData(LID lid) {myLid= new LID(lid.pageNo, lid.slotNo);};

  /** get a copy of the lid
  *  @return the reference of the copy 
  */
  public LID getData() {return new LID(myLid.pageNo, myLid.slotNo);};

  /** set the lid
   */ 
  public void setData(LID lid) { myLid= new LID(lid.pageNo, lid.slotNo);};
<<<<<<< HEAD
}   
=======
}   
>>>>>>> main
