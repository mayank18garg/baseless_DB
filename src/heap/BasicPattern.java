/* File Tuple.java */

package heap;

import global.*;

import java.io.IOException;

import labelheap.*;

public class BasicPattern implements GlobalConst {

	public static SystemDefs sysdef = null;

	/**
	 * Maximum size of any tuple
	 */
	public static final int max_size = MINIBASE_PAGESIZE;

	/**
	 * a byte array to hold data
	 */
	private byte[] data;

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

	private short[] fldOffset;

	/**
	 * private field
	 * default confidence
	 */

	// private double confidence;

	/**
	 * Class constructor
	 * Creat a new tuple with length = max_size,tuple offset = 0.
	 */

	public BasicPattern() {
		// Creat a new tuple
		data = new byte[max_size];
		bp_offset = 0;
		bp_length = max_size;
	}

	/**
	 * Constructor
	 * 
	 * @param abp    a byte array which contains the tuple
	 * @param offset the offset of the tuple in the byte array
	 * @param length the length of the tuple
	 */

	public BasicPattern(byte[] abp, int offset, int length) {
		data = abp;
		bp_offset = offset;
		bp_length = length;
		// fldCnt = (short) (length/4); //length is the byte array size, 4 is the number
		// of bytes required to store an integer
		// setFldOffset(fldCnt/2, offset);
	}

	// private void setFldOffset(int numOfNodes, int offset) {

	// fldOffset[0] = (short)(offset);
	// for(int i = 1;i<numOfNodes;i++){
	// fldOffset[i] = (short) (fldOffset[i-1]+8);
	// }
	// }

	/**
	 * Constructor(used as tuple copy)
	 * 
	 * @param fromBasicPattern a byte array which contains the tuple
	 *
	 */
	public BasicPattern(BasicPattern fromBasicPattern) {
		data = fromBasicPattern.getBPByteArray();
		bp_length = fromBasicPattern.getLength();
		bp_offset = 0;
		fldCnt = fromBasicPattern.noOfFlds();
		fldOffset = fromBasicPattern.copyFldOffset();
	}

	public void SetBasicPatternArray(byte[] recordarray) {
		data = recordarray;
	}

	public BasicPattern(int size) {
		// Creat a new tuple
		data = new byte[size];
		bp_offset = 0;
		bp_length = size;
	}

	public BasicPattern(Tuple tuple) {

		data = new byte[max_size];
		bp_offset = 0;
		bp_length = max_size;

		try {
			int no_tuple_fields = tuple.noOfFlds();
			setHdr((short) ((no_tuple_fields - 1) / 2 + 1));
			int j = 1;
			for (int i = 1; i < fldCnt; i++) {
				int slotno = tuple.getIntFld(j++);
				int pageno = tuple.getIntFld(j++);
				PageId page = new PageId(pageno);
				LID lid = new LID(page, slotno);
				EID eid = lid.returnEID();
				setEIDIntFld(i, eid);
			}
			setDoubleFld(fldCnt, (double) (tuple.getFloFld(j)));
		} catch (Exception e) {
			System.out.println("Error creating basic pattern from tuple" + e);
		}
	}

	public EID getEID(int fldNo)
			throws IOException, FieldNumberOutOfBoundException {
		if ((fldNo > 0) && (fldNo <= fldCnt)) {
			int slotno = Convert.getIntValue(fldOffset[fldNo - 1], data);
			int pageno = Convert.getIntValue(fldOffset[fldNo - 1] + 4, data);
			PageId pageid = new PageId(pageno);
			LID lid = new LID(pageid, slotno);
			EID eid = new EID(lid);
			return eid;
		} else
			throw new FieldNumberOutOfBoundException(null, "BASICPATTERN:TUPLE_FLDNO_OUT_OF_BOUND");
	}

	public double getDoubleFld(int fldNo)
			throws IOException, FieldNumberOutOfBoundException {
		double val;
		if ((fldNo > 0) && (fldNo <= fldCnt)) {
			val = (double) (Convert.getFloValue(fldOffset[fldNo - 1], data));
			return val;
		} else
			throw new FieldNumberOutOfBoundException(null, "BP:BasicPattern_FLDNO_OUT_OF_BOUND");
	}

	public int getLength() {
		return bp_length;
	}

	// public BasicPattern setIntFld(int fldNo, NID nid)
	// throws IOException, FieldNumberOutOfBoundException {
	// if ((fldNo > 0) && (fldNo <= fldCnt)) {
	// Convert.setIntValue(nid.slotNo, fldOffset[fldNo - 1], data);
	// Convert.setIntValue(nid.pageNo.pid, fldOffset[fldNo - 1] + 4, data);
	// return this;
	// } else
	// throw new FieldNumberOutOfBoundException(null,
	// "BASICPATTERN:TUPLE_FLDNO_OUT_OF_BOUND");
	// }

	public Tuple getTuplefromBasicPattern() {
		Tuple tuple1 = new Tuple();
		int length = (fldCnt);
		AttrType[] types = new AttrType[(length - 1) * 2 + 1];
		int j = 0;
		for (j = 0; j < (length - 1) * 2; j++) {
			types[j] = new AttrType(AttrType.attrInteger);
		}
		types[j] = new AttrType(AttrType.attrReal);

		short[] s_sizes = new short[1];
		s_sizes[0] = (short) ((length - 1) * 2 * 4 + 1 * 8);
		try {
			tuple1.setHdr((short) ((length - 1) * 2 + 1), types, s_sizes);
		} catch (InvalidTypeException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InvalidTupleSizeException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int i = 0;
		j = 1;
		for (i = 0; i < fldCnt - 1; i++) {
			try {
				EID eid = getEID(i + 1);
				tuple1.setIntFld(j++, eid.slotNo);
				tuple1.setIntFld(j++, eid.pageNo.pid);
			} catch (FieldNumberOutOfBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			tuple1.setFloFld(j, (float) (getDoubleFld(fldCnt)));
		} catch (FieldNumberOutOfBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*
		 * try {
		 * tuple1.print(types);
		 * } catch (IOException e) {
		 * // TODO Auto-generated catch block
		 * e.printStackTrace();
		 * }
		 */
		return tuple1;
	}

	public BasicPattern setEIDIntFld(int fldNo, EID eid)
			throws IOException, FieldNumberOutOfBoundException {
		if ((fldNo > 0) && (fldNo <= fldCnt)) {
			Convert.setIntValue(eid.slotNo, fldOffset[fldNo - 1], data);
			Convert.setIntValue(eid.pageNo.pid, fldOffset[fldNo - 1] + 4, data);
			return this;
		} else
			throw new FieldNumberOutOfBoundException(null, "BASICPATTERN:TUPLE_FLDNO_OUT_OF_BOUND");
	}

	public BasicPattern setDoubleFld(int fldNo, double val)
			throws IOException, FieldNumberOutOfBoundException {
		if ((fldNo > 0) && (fldNo <= fldCnt)) {
			Convert.setFloValue((float) (val), fldOffset[fldNo - 1], data);
			return this;
		} else
			throw new FieldNumberOutOfBoundException(null, "BasicPattern:BASIC PATTERN_FLDNO_OUT_OF_BOUND");

	}

	/**
	 * Copy a tuple to the current tuple position
	 * you must make sure the tuple lengths must be equal
	 * 
	 * @param fromBasicPattern the tuple being copied
	 */
	public void basicPatternCopy(BasicPattern fromBasicPattern) {
		byte[] temparray = fromBasicPattern.getBPByteArray();
		System.arraycopy(temparray, 0, data, bp_offset, bp_length);
		// fldCnt = fromBasicPattern.noOfFlds();
		// fldOffset = fromBasicPattern.copyFldOffset();
	}

	/**
	 * This is used when you don't want to use the constructor
	 * 
	 * @param abp    a byte array which contains the tuple
	 * @param offset the offset of the tuple in the byte array
	 * @param length the length of the tuple
	 */

	public void basicPatternInit(byte[] abp, int offset, int length) {
		data = abp;
		bp_offset = offset;
		bp_length = length;
		// fldCnt = (short) (length/4); //length is the byte array size, 4 is the number
		// of bytes required to store an integer
		// setFldOffset(fldCnt/2, offset);
	}

	/**
	 * Set a tuple with the given tuple length and offset
	 * 
	 * @param record a byte array contains the tuple
	 * @param offset the offset of the tuple ( =0 by default)
	 * @param length the length of the tuple
	 */
	public void basicPatternSet(byte[] record, int offset, int length) {
		System.arraycopy(record, offset, data, 0, length);
		bp_offset = 0;
		bp_length = length;
		// fldCnt = (short) (length/4); //length is the byte array size, 4 is the number
		// of bytes required to store an integer
		// setFldOffset(fldCnt/2, offset);
	}

	/**
	 * get the length of a tuple, call this method if you did
	 * call setHdr () before
	 * 
	 * @return size of this tuple in bytes
	 */
	public short size() {
		return ((short) (fldOffset[fldCnt] - bp_offset));
	}

	/**
	 * get the offset of a tuple
	 * 
	 * @return offset of the tuple in byte array
	 */
	public int getOffset() {
		return bp_offset;
	}

	/**
	 * Copy the tuple byte array out
	 * 
	 * @return byte[], a byte array contains the tuple
	 *         the length of byte[] = length of the tuple
	 */

	public byte[] getBPByteArray() {
		byte[] bpcopy = new byte[bp_length];
		System.arraycopy(data, bp_offset, bpcopy, 0, bp_length);
		return bpcopy;
	}

	/**
	 * return the data byte array
	 * 
	 * @return data byte array
	 */

	public byte[] returnBPByteArray() {
		return data;
	}

	public short noOfFlds() {
		return fldCnt;
	}

	/**
	 * Makes a copy of the fldOffset array
	 *
	 * @return a copy of the fldOffset arrray
	 *
	 */

	public short[] copyFldOffset() {
		short[] newFldOffset = new short[fldCnt + 1];
		for (int i = 0; i <= fldCnt; i++) {
			newFldOffset[i] = fldOffset[i];
		}

		return newFldOffset;
	}

	public void setHdr(short numFlds)
			throws IOException, InvalidTypeException, InvalidTupleSizeException {
		if ((numFlds + 2) * 2 > max_size)
			throw new InvalidTupleSizeException(null, "TUPLE: TUPLE_TOOBIG_ERROR");

		fldCnt = numFlds;
		Convert.setShortValue(numFlds, bp_offset, data);
		fldOffset = new short[numFlds + 1];
		int pos = bp_offset + 2; // start position for fldOffset[]

		// sizeof short =2 +2: array siaze = numFlds +1 (0 - numFilds) and
		// another 1 for fldCnt
		fldOffset[0] = (short) ((numFlds + 2) * 2 + bp_offset);

		Convert.setShortValue(fldOffset[0], pos, data);
		pos += 2;
		short incr;
		int i;

		for (i = 1; i < numFlds; i++) {
			// 4 + 4 page no and slot no respectively
			incr = 8;
			fldOffset[i] = (short) (fldOffset[i - 1] + incr);
			Convert.setShortValue(fldOffset[i], pos, data);
			pos += 2;

		}
		incr = 8;

		fldOffset[numFlds] = (short) (fldOffset[i - 1] + incr);
		Convert.setShortValue(fldOffset[numFlds], pos, data);

		bp_length = fldOffset[numFlds] - bp_offset;

		if (bp_length > max_size)
			throw new InvalidTupleSizeException(null, "TUPLE: TUPLE_TOOBIG_ERROR");
	}

	/**
	 * Print out the tuple
	 * 
	 * @Exception IOException I/O exception
	 */
	public void print()
			throws IOException {
		LabelHeapfile Entity_HF = sysdef.JavabaseDB.getEntityHandle();
		System.out.print("[");
		try {
			for (int i = 1; i <= fldCnt - 1; i++) {
				Label subject = Entity_HF.getLabel(this.getEID(i).returnLID());
				System.out.printf("%30s  ", subject.getLabel());
			}
			System.out.print(getDoubleFld(fldCnt));
			System.out.println("]");

		} catch (InvalidSlotNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTupleSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFDiskMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFBufMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FieldNumberOutOfBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
