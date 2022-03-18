package iterator;

import heap.*;
import labelheap.Label;
import global.*;
import java.io.*;
import java.lang.*;

/**
 * some useful method when processing Tuple
 */
public class LabelUtils {

  /**
   * This function compares a tuple with another tuple in respective field, and
   *  returns:
   *
   *    0        if the two are equal,
   *    1        if the tuple is greater,
   *   -1        if the tuple is smaller,
   *
   *@param    fldType   the type of the field being compared.
   *@param    t1        one tuple.
   *@param    t2        another tuple.
   *@param    t1_fld_no the field numbers in the tuples to be compared.
   *@param    t2_fld_no the field numbers in the tuples to be compared. 
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   *@return   0        if the two are equal,
   *          1        if the tuple is greater,
   *         -1        if the tuple is smaller,                              
   */
  public static int CompareLabelWithLabel(Label l1, Label l2)
    throws IOException,UnknowAttrType,TupleUtilsException,FieldNumberOutOfBoundException
    {
        String l1_s, l2_s;
        l1_s = l1.getLabel();
        l2_s = l2.getLabel();
	  // Now handle the special case that is posed by the max_values for strings...
	    if(l1_s.compareTo(l2_s)>0)return 1;
	    if(l1_s.compareTo(l2_s)<0)return -1;
	    return 0;
    }


    /**
     * This function Compares two Tuple inn all fields
     * 
     * @param t1     the first tuple
     * @param t2     the secocnd tuple
     * @param type[] the field types
     * @param len    the field numbers
     * @return 0 if the two are not equal,
     *         1 if the two are equal,
     * @exception UnknowAttrType      don't know the attribute type
     * @exception IOException         some I/O fault
     * @exception TupleUtilsException exception from this class
     * @throws FieldNumberOutOfBoundException
     */

    public static boolean Equal(Label l1, Label l2)
    throws IOException, UnknowAttrType, TupleUtilsException, FieldNumberOutOfBoundException 
    {
        int value = CompareLabelWithLabel(l1, l2);
        if(value==0) return true;
        return false;
    }

    /**
     * get the string specified by the field number
     * 
     * @param tuple the tuple
     * @param fidno the field number
     * @return the content of the field number
     * @exception IOException         some I/O fault
     * @exception TupleUtilsException exception from this class
     */
    public static String Value(Label label)
            throws IOException,
            TupleUtilsException {
        String temp;
        temp = label.getLabel();
        return temp;
    }

    /**
     * set up a tuple in specified field from a tuple
     * 
     * @param value   the tuple to be set
     * @param tuple   the given tuple
     * @param fld_no  the field number
     * @param fldType the tuple attr type
     * @exception UnknowAttrType      don't know the attribute type
     * @exception IOException         some I/O fault
     * @exception TupleUtilsException exception from this class
     */
    public static void SetValue(Label value, Label label)
            throws IOException,
            UnknowAttrType,
            TupleUtilsException {
        value.setLabel(label.getLabel());
        return;
    }

    /**
     * set up the Jtuple's attrtype, string size,field number for using join
     * 
     * @param Jtuple       reference to an actual tuple - no memory has been
     *                     malloced
     * @param res_attrs    attributes type of result tuple
     * @param in1          array of the attributes of the tuple (ok)
     * @param len_in1      num of attributes of in1
     * @param in2          array of the attributes of the tuple (ok)
     * @param len_in2      num of attributes of in2
     * @param t1_str_sizes shows the length of the string fields in S
     * @param t2_str_sizes shows the length of the string fields in R
     * @param proj_list    shows what input fields go where in the output tuple
     * @param nOutFlds     number of outer relation fileds
     * @exception IOException         some I/O fault
     * @exception TupleUtilsException exception from this class
     */
 /*   public static short[] setup_op_tuple(Tuple Jtuple, AttrType[] res_attrs,
            AttrType in1[], int len_in1, AttrType in2[],
            int len_in2, short t1_str_sizes[],
            short t2_str_sizes[],
            FldSpec proj_list[], int nOutFlds)
            throws IOException,
            TupleUtilsException {
        short[] sizesT1 = new short[len_in1];
        short[] sizesT2 = new short[len_in2];
        int i, count = 0;

        for (i = 0; i < len_in1; i++)
            if (in1[i].attrType == AttrType.attrString)
                sizesT1[i] = t1_str_sizes[count++];

        for (count = 0, i = 0; i < len_in2; i++)
            if (in2[i].attrType == AttrType.attrString)
                sizesT2[i] = t2_str_sizes[count++];

        int n_strs = 0;
        for (i = 0; i < nOutFlds; i++) {
            if (proj_list[i].relation.key == RelSpec.outer)
                res_attrs[i] = new AttrType(in1[proj_list[i].offset - 1].attrType);
            else if (proj_list[i].relation.key == RelSpec.innerRel)
                res_attrs[i] = new AttrType(in2[proj_list[i].offset - 1].attrType);
        }

        // Now construct the res_str_sizes array.
        for (i = 0; i < nOutFlds; i++) {
            if (proj_list[i].relation.key == RelSpec.outer
                    && in1[proj_list[i].offset - 1].attrType == AttrType.attrString)
                n_strs++;
            else if (proj_list[i].relation.key == RelSpec.innerRel
                    && in2[proj_list[i].offset - 1].attrType == AttrType.attrString)
                n_strs++;
        }

        short[] res_str_sizes = new short[n_strs];
        count = 0;
        for (i = 0; i < nOutFlds; i++) {
            if (proj_list[i].relation.key == RelSpec.outer
                    && in1[proj_list[i].offset - 1].attrType == AttrType.attrString)
                res_str_sizes[count++] = sizesT1[proj_list[i].offset - 1];
            else if (proj_list[i].relation.key == RelSpec.innerRel
                    && in2[proj_list[i].offset - 1].attrType == AttrType.attrString)
                res_str_sizes[count++] = sizesT2[proj_list[i].offset - 1];
        }
        try {
            Jtuple.setHdr((short) nOutFlds, res_attrs, res_str_sizes);
        } catch (Exception e) {
            throw new TupleUtilsException(e, "setHdr() failed");
        }
        return res_str_sizes;
    }
*/
    /**
     * set up the Jtuple's attrtype, string size,field number for using project
     * 
     * @param Jtuple       reference to an actual tuple - no memory has been
     *                     malloced
     * @param res_attrs    attributes type of result tuple
     * @param in1          array of the attributes of the tuple (ok)
     * @param len_in1      num of attributes of in1
     * @param t1_str_sizes shows the length of the string fields in S
     * @param proj_list    shows what input fields go where in the output tuple
     * @param nOutFlds     number of outer relation fileds
     * @exception IOException         some I/O fault
     * @exception TupleUtilsException exception from this class
     * @exception InvalidRelation     invalid relation
     */

  /*  public static short[] setup_op_tuple(Tuple Jtuple, AttrType res_attrs[],
            AttrType in1[], int len_in1,
            short t1_str_sizes[],
            FldSpec proj_list[], int nOutFlds)
            throws IOException,
            TupleUtilsException,
            InvalidRelation {
        short[] sizesT1 = new short[len_in1];
        int i, count = 0;

        for (i = 0; i < len_in1; i++)
            if (in1[i].attrType == AttrType.attrString)
                sizesT1[i] = t1_str_sizes[count++];

        int n_strs = 0;
        for (i = 0; i < nOutFlds; i++) {
            if (proj_list[i].relation.key == RelSpec.outer)
                res_attrs[i] = new AttrType(in1[proj_list[i].offset - 1].attrType);

            else
                throw new InvalidRelation("Invalid relation -innerRel");
        }

        // Now construct the res_str_sizes array.
        for (i = 0; i < nOutFlds; i++) {
            if (proj_list[i].relation.key == RelSpec.outer
                    && in1[proj_list[i].offset - 1].attrType == AttrType.attrString)
                n_strs++;
        }

        short[] res_str_sizes = new short[n_strs];
        count = 0;
        for (i = 0; i < nOutFlds; i++) {
            if (proj_list[i].relation.key == RelSpec.outer
                    && in1[proj_list[i].offset - 1].attrType == AttrType.attrString)
                res_str_sizes[count++] = sizesT1[proj_list[i].offset - 1];
        }

        try {
            Jtuple.setHdr((short) nOutFlds, res_attrs, res_str_sizes);
        } catch (Exception e) {
            throw new TupleUtilsException(e, "setHdr() failed");
        }
        return res_str_sizes;
    }
    
    */
}
