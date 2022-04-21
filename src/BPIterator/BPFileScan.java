package BPIterator;

import java.io.IOException;

import bufmgr.PageNotReadException;
import global.*;
import heap.*;
import iterator.*;

/**
 * open a heapfile and according to the condition expression to get
 * output file, call get_next to get all tuples
 */
public class BPFileScan extends BPIterator {
	// private AttrType[] _in1;
	// private short in1_len;

	private Heapfile f;
	private Scan scan;
	private Tuple tuple1;
	// private int t1_size;
	// private int nOutFlds;
	// private CondExpr[] OutputFilter;
	// public FldSpec[] perm_mat;
	// public int totalNumberOfAttributes;
	public int length;
	public AttrType[] types;
	private short[] s_sizes;

	/**
	 * constructor
	 * 
	 * @param file_name  heapfile to be opened
	 * @param n_out_flds number of fields in the out tuple
	 * @exception IOException         some I/O fault
	 * @exception FileScanException   exception from this class
	 * @exception TupleUtilsException exception from this class
	 * @exception InvalidRelation     invalid relation
	 */
	public BPFileScan(String file_name, int n_out_flds)
			throws IOException, FileScanException {

		tuple1 = new Tuple();
		// nOutFlds = n_out_flds;

		try {
			f = new Heapfile(file_name);
		} catch (Exception e) {
			throw new FileScanException(e, "Create new heapfile failed");
		}
		length = n_out_flds;
		types = new AttrType[length * 2];
		for (int i = 0; i < length * 2; i++) {
			types[i] = new AttrType(AttrType.attrInteger);
		}

		try {
			tuple1.setHdr((short) (length * 2), types, s_sizes);
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
	 * @return the result tuple
	 * @exception JoinsException                 some join exception
	 * @exception IOException                    I/O errors
	 * @exception InvalidTupleSizeException      invalid tuple size
	 * @exception InvalidTypeException           tuple type not valid
	 * @exception PageNotReadException           exception from lower layer
	 * @exception PredEvalException              exception from PredEval class
	 * @exception UnknowAttrType                 attribute type unknown
	 * @exception FieldNumberOutOfBoundException array out of bounds
	 * @exception WrongPermat                    exception for wrong FldSpec
	 *                                           argument
	 */
	@Override
	public BasicPattern getnext()
			throws JoinsException, IOException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException,
			PredEvalException, UnknowAttrType, FieldNumberOutOfBoundException, WrongPermat {

		RID rid = new RID();

		while (true) {

			if ((tuple1 = scan.getNext(rid)) == null) {
				return null;
			}

			tuple1.setHdr((short) (length * 2), types, s_sizes);
			short len = tuple1.noOfFlds();

			BasicPattern bp = new BasicPattern();
			try {
				bp.setHdr((short) (len / 2));
			} catch (InvalidTupleSizeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
	 * implement the abstract method close() from super class Iterator
	 * to finish cleaning up
	 */
	public void close() {

		if (!closeFlag) {
			scan.closescan();
			closeFlag = true;
		}

	}

}