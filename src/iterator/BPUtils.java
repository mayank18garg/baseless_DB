package iterator;

import global.*;
import heap.FieldNumberOutOfBoundException;
import heap.Tuple;
import labelheap.Label;
import labelheap.LabelHeapfile;
import quadrupleheap.Quadruple;

import java.io.IOException;

import static iterator.TupleUtils.CompareTupleWithTuple;

public class BPUtils {

    /**
     * compare quadruples based on the subject value
     *
     *
     * @return return 1 if q1 is greater than q2
     * return -1 if q1 is less than q2
     */
    private static int compareTuple(AttrType field_type, Tuple Tuple1, int Field_no_1, Tuple Tuple2, int Field_no_2) throws Exception {
        int pid_1, sid_1, pid_2, sid_2;

        String t1_s, t2_s;
        char compare_min[] = new char[1];
        char compare_max[] = new char[1];
        compare_min[0] = Character.MIN_VALUE;
        compare_max[0] = Character.MAX_VALUE;
        String string_min = new String(compare_min);
        String string_max = new String(compare_max);
        LabelHeapfile entity_label_Heap = SystemDefs.JavabaseDB.getEntityHandle();

        switch (field_type.attrType) {
            case AttrType.attrInteger:                // Compare two integers.
                {
                    sid_1 = Tuple1.getIntFld(Field_no_1);
                    pid_1 = Tuple1.getIntFld(Field_no_1 + 1);
                    sid_2 = Tuple2.getIntFld(Field_no_2);
                    pid_2 = Tuple2.getIntFld(Field_no_2 + 1);
                    PageId p_pid_1 = new PageId(pid_1);
                    PageId p_pid_2 = new PageId(pid_2);
                    LID lid_1 = new LID(p_pid_1, sid_1);
                    LID lid_2 = new LID(p_pid_2, sid_2);
                    Label entity1, entity2;
                    if (lid_1.pageNo.pid < 0) return -1;
                    else if (lid_1.pageNo.pid == Integer.MAX_VALUE) return 1;
                    else {
                        entity1 = entity_label_Heap.getLabel(lid_1);              // Comparing Entities
                        t1_s = entity1.getLabelKey();
                    }
                    if (lid_2.pageNo.pid < 0) return 1;
                    else if (lid_2.pageNo.pid == Integer.MAX_VALUE) return -1;
                    else {
                        entity2 = entity_label_Heap.getLabel(lid_2);
                        t2_s = entity2.getLabelKey();
                    }
                } if (t1_s.compareTo(t2_s) > 0) return 1;
                if (t1_s.compareTo(t2_s) < 0) return -1;
                return 0;

            case AttrType.attrReal: // Compare two floats
                double t1_real, t2_real;
                 {
                    t1_real = Tuple1.getFloFld(Field_no_1);
                    t2_real = Tuple2.getFloFld(Field_no_2);
                } if (t1_real == t2_real) return 0;
                if (t1_real < t2_real) return -1;
                if (t1_real > t2_real) return 1;

            default:

                throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");

        }
    }


    public static int CompareTupleWithValue(AttrType field_type, Tuple Tuple1, int Field_no_1, Tuple value) {
        return CompareTupleWithValue(field_type, Tuple1, Field_no_1, value);
    }


    public static void SetValue(AttrType field_type, Tuple Tuple1, int Field_no_1, Tuple value) throws IOException, UnknowAttrType, FieldNumberOutOfBoundException {

        switch (field_type.attrType) {
            case AttrType.attrInteger:
                value.setIntFld(Field_no_1, Tuple1.getIntFld(Field_no_1));
                value.setIntFld(Field_no_1 + 1, Tuple1.getIntFld(Field_no_1 + 1));
                break;
            case AttrType.attrReal:

                value.setFloFld(Field_no_1, Tuple1.getFloFld(Field_no_1));
                break;
            default:
                throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");

        }

        return;
    }
}