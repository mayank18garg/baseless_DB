package global;

import java.io.IOException;

public class NID {

    /** public int slotNo
     */
    public int slotNo;

    /** public PageId pageNo
     */
    public PageId pageNo = new PageId();

    /**
     * default constructor of class
     */
    public NID() { }

    /**
     *  constructor of class
     */
    public NID(LID lid)
    {
        this.pageNo = lid.pageNo;
        this.slotNo = lid.slotNo;
    }

    public NID(PageId pageid, int slotno) {
        pageNo = pageid;
        slotNo = slotNo;
    }

    /**
     * make a copy of the given eid
     */
    public void copyNid (NID nid)
    {
        pageNo = nid.pageNo;
        slotNo = nid.slotNo;
    }

    /** Write the eid into a byte array at offset
     * @param ary the specified byte array
     * @param offset the offset of byte array to write
     * @exception IOException I/O errors
     */
    public void writeToByteArray(byte [] ary, int offset)
            throws IOException
    {
        Convert.setIntValue ( slotNo, offset, ary);
        Convert.setIntValue ( pageNo.pid, offset+4, ary);
    }


    /** Compares two EID object, i.e, this to the eid
     * @param nid EID object to be compared to
     * @return true is they are equal
     *         false if not.
     */
    public boolean equals(NID nid) {

        if ((this.pageNo.pid==nid.pageNo.pid)
                &&(this.slotNo==nid.slotNo))
            return true;
        else
            return false;
    }

    public LID returnLID(){
        return new LID(this.pageNo, this.slotNo);
    }
}
