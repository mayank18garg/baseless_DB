package BPIterator;

import java.io.*;
import global.*;
import heap.BasicPattern;
import heap.*;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.OBuf;
import iterator.SpoofIbuf;
import iterator.UnknowAttrType;

/**
 * The Sort class sorts a file. All necessary information are passed as
 * arguments to the constructor. After the constructor call, the user can
 * repeatly call <code>get_next()</code> to get tuples in sorted order.
 * After the sorting is done, the user should call <code>close()</code>
 * to clean up.
 */
public class BPSort extends BPIterator implements GlobalConst
{
    private static final int ARBIT_RUNS = 10;

    private AttrType[]  _in;
    private short       n_cols;
    private short[]     str_lens;
    private BPFileScan   _am;
    private int         _sort_fld;
    private BPOrder  order;
    private int         _n_pages;
    private byte[][]    bufs;
    private boolean     first_time;
    private int         Nruns;
    private int         max_elems_in_heap;
    private int         tuple_size;

    private BPpnodeSplayPQ Q;
    private Heapfile[]   temp_files;
    private int          n_tempfiles;
    private Tuple        output_tuple;
    private int[]        n_tuples;
    private int          n_runs;
    private Tuple        op_buf;
    private OBuf         o_buf;
    private SpoofIbuf[]  i_buf;
    private PageId[]     bufs_pids;
    private boolean useBM = true; // flag for whether to use buffer manager

    /**
     * Set up for merging the runs.
     * Open an input buffer for each run, and insert the first element (min)
     * from each run into a heap. <code>delete_min() </code> will then get
     * the minimum of all runs.
     * @param tuple_size size (in bytes) of each tuple
     * @param n_R_runs number of runs
     * @exception IOException from lower layers
     * @exception LowMemException there is not enough memory to
     *                 sort in two passes (a subclass of BPSortException).
     * @exception BPSortException something went wrong in the lower layer.
     * @exception Exception other exceptions
     */
    private void setup_for_merge(int tuple_size, int n_R_runs)
            throws IOException,
            LowMemException,
            BPSortException,
            Exception
    {
        if (n_R_runs > _n_pages)
            throw new LowMemException("Sort.java: Not enough memory to sort in two passes.");

        int i;
        BPpnode cur_node;  

        i_buf = new SpoofIbuf[n_R_runs];   
        for (int j=0; j<n_R_runs; j++) i_buf[j] = new SpoofIbuf();

        for (i=0; i<n_R_runs; i++) {
            byte[][] apage = new byte[1][];
            apage[0] = bufs[i];

           
            i_buf[i].init(temp_files[i], apage, 1, tuple_size, n_tuples[i]);

            cur_node = new BPpnode();
            cur_node.run_num = i;

            // may need change depending on whether Get() returns the original
            // or make a copy of the tuple, need io_bufs.java ???
            Tuple temp_tuple = new Tuple(tuple_size);

            try {
                temp_tuple.setHdr(n_cols, _in, str_lens);
            }
            catch (Exception e) {
                throw new BPSortException(e, "Sort.java: Tuple.setHdr() failed");
            }

            temp_tuple =i_buf[i].Get(temp_tuple);  

            if (temp_tuple != null) {
                /*
                System.out.print("Get tuple from run " + i);
                temp_tuple.print(_in);
                */
                cur_node.tuple = temp_tuple; 
                try {
                    Q.enq(cur_node);
                }
                catch (UnknowAttrType e) {
                    throw new BPSortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                }
                catch (BasicPatternUtilsException e) {
                    throw new BPSortException(e, "Sort.java: BasicPatternUtilsException caught from Q.enq()");
                }

            }
        }
        return;
    }

    public void sort_init(AttrType[] in,short len_in, short[] str_sizes)
            throws IOException, BPSortException
    {
        _in = new AttrType[len_in];
        n_cols = len_in;
        int n_strs = 0;

        for (int i=0; i<len_in; i++)
        {
            _in[i] = new AttrType(in[i].attrType);

            if (in[i].attrType == AttrType.attrString)
            {
                n_strs ++;
            }
        }

        str_lens = new short[n_strs];

        n_strs = 0;
        for (int i=0; i<len_in; i++)
        {
            if (_in[i].attrType == AttrType.attrString)
            {
                str_lens[n_strs] = str_sizes[n_strs];
                n_strs ++;
            }
        }

        Tuple t = new Tuple(); 
        try
        {
            t.setHdr(len_in, _in, str_sizes);
        }
        catch (Exception e)
        {
            throw new BPSortException(e, "Sort.java: t.setHdr() failed");
        }
        tuple_size = t.size();

        bufs_pids = new PageId[_n_pages];
        bufs = new byte[_n_pages][];

        if (useBM)
        {
            try
            {
                get_buffer_pages(_n_pages, bufs_pids, bufs);
            }
            catch (Exception e) {
                throw new BPSortException(e, "Sort.java: BUFmgr error");
            }
        }
        else
        {
            for (int k=0; k<_n_pages; k++) bufs[k] = new byte[MAX_SPACE];
        }

        // as a heuristic, we set the number of runs to an arbitrary value
        // of ARBIT_RUNS
        temp_files = new Heapfile[ARBIT_RUNS];
        n_tempfiles = ARBIT_RUNS;
        n_tuples = new int[ARBIT_RUNS];
        n_runs = ARBIT_RUNS;

        try
        {
            temp_files[0] = new Heapfile(null);
        }
        catch (Exception e) {
            throw new BPSortException(e, "Sort.java: Heapfile error");
        }

        o_buf = new OBuf();

        o_buf.init(bufs, _n_pages, tuple_size, temp_files[0], false);	
        //    output_tuple = null;

        Q = new BPpnodeSplayPQ(_sort_fld, in[_sort_fld - 1], order);		

        op_buf = new Tuple(tuple_size);   
        try {
            op_buf.setHdr(n_cols, _in, str_lens);
        }
        catch (Exception e) {
            throw new BPSortException(e, "Sort.java: op_buf.setHdr() failed");
        }
    }

    /**
     * Generate sorted runs.
     * Using heap sort.
     * @param  max_elems    maximum number of elements in heap
     * @param  sortFldType  attribute type of the sort field
     * @return number of runs generated
     * @exception IOException from lower layers
     * @exception BPSortException something went wrong in the lower layer.
     * @exception JoinsException from <code>Iterator.get_next()</code>
     */
    private int generate_runs(int max_elems, AttrType sortFldType)
            throws IOException,
            BPSortException,
            UnknowAttrType,
            BasicPatternUtilsException,
            JoinsException,
            Exception
    {
        int init_flag = 1;
        BasicPattern bp;
        Tuple tuple;
        BPpnode cur_node;
        BPpnodeSplayPQ Q1 = new BPpnodeSplayPQ(_sort_fld, sortFldType, order);
        BPpnodeSplayPQ Q2 = new BPpnodeSplayPQ(_sort_fld, sortFldType, order);
        BPpnodeSplayPQ pcurr_Q = Q1;
        BPpnodeSplayPQ pother_Q = Q2;

        int run_num = 0;  // keeps track of the number of runs

        // number of elements in Q
        //    int nelems_Q1 = 0;
        //    int nelems_Q2 = 0;
        int p_elems_curr_Q = 0;
        int p_elems_other_Q = 0;

        int comp_res;

        // maintain a fixed maximum number of elements in the heap
        while ((p_elems_curr_Q + p_elems_other_Q) < max_elems)
        {
            try
            {
                bp = _am.getnext();
                if(bp!=null)
                    tuple = bp.getTuplefromBasicPattern();  
                else
                    tuple = null;
                if(init_flag==1)
                {
                    AttrType[] in = new AttrType[tuple.noOfFlds()];
                    int j = 0;
                    for(j = 0 ; j < (tuple.noOfFlds()-1)  ; j++)
                    {
                        in[j] = new AttrType(AttrType.attrInteger);
                    }
                    in[j] = new AttrType(AttrType.attrReal);

                    short[] s_sizes = new short[1];
                    s_sizes[0] = (short)((tuple.noOfFlds()-1) * 4 + 1* 8);

                    if (_sort_fld==-1)
                    {
                        _sort_fld = tuple.noOfFlds();
                        BPpnodeSplayPQ Q1_confidence = new BPpnodeSplayPQ(_sort_fld, sortFldType, order);
                        BPpnodeSplayPQ Q2_confidence = new BPpnodeSplayPQ(_sort_fld, sortFldType, order);
                        pcurr_Q = Q1_confidence;
                        pother_Q = Q2_confidence;
                    }

                    sort_init(in,tuple.noOfFlds(),s_sizes);
                    init_flag = 0;
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
                throw new BPSortException(e, "Sort.java: get_next() failed");
            }

            if (tuple == null)
            {
                break;
            }
            cur_node = new BPpnode();
            cur_node.tuple = new Tuple(tuple); 

            pcurr_Q.enq(cur_node);
            p_elems_curr_Q ++;
        }

        Tuple lastElem = new Tuple(tuple_size);  
        try
        {
            lastElem.setHdr(n_cols, _in, str_lens);
        }
        catch (Exception e)
        {
            throw new BPSortException(e, "Sort.java: setHdr() failed");
        }

        // set the lastElem to be the minimum value for the sort field
        if(order.bpOrder == BPOrder.Ascending)
        {
            try
            {
                MIN_VAL(lastElem, sortFldType);
            }
            catch (UnknowAttrType e)
            {
                throw new BPSortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
            }
            catch (Exception e)
            {
                throw new BPSortException(e, "MIN_VAL failed");
            }
        }
        else
        {
            try
            {
                MAX_VAL(lastElem, sortFldType);
            }
            catch (UnknowAttrType e)
            {
                throw new BPSortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
            }
            catch (Exception e)
            {
                throw new BPSortException(e, "MIN_VAL failed");
            }
        }

        // now the queue is full, starting writing to file while keep trying
        // to add new tuples to the queue. The ones that does not fit are put
        // on the other queue temperarily
        while (true)
        {
            cur_node = pcurr_Q.deq();
            if (cur_node == null) break;
            p_elems_curr_Q --;

            comp_res = BPUtils.CompareTupleWithValue(sortFldType, cur_node.tuple, _sort_fld, lastElem);  // need tuple_utils.java

            if ((comp_res < 0 && order.bpOrder == BPOrder.Ascending) || (comp_res > 0 && order.bpOrder == BPOrder.Descending))
            {
                // doesn't fit in current run, put into the other queue
                try
                {
                    pother_Q.enq(cur_node);
                }
                catch (UnknowAttrType e)
                {
                    throw new BPSortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                }
                p_elems_other_Q ++;
            }
            else
            {
                // set lastElem to have the value of the current tuple,
                BPUtils.SetValue(lastElem, cur_node.tuple, _sort_fld, sortFldType);
                //BPUtils.SetValue(sortFldType, lastElem, _sort_fld, cur_node.tuple);
                //write tuple to output file, need io_bufs.java, type cast???
                //System.out.println("Putting tuple into run " + (run_num + 1));
                //cur_node.tuple.print(_in);

                o_buf.Put(cur_node.tuple);
            }

            // check whether the other queue is full
            if (p_elems_other_Q == max_elems)
            {
                // close current run and start next run
                n_tuples[run_num] = (int) o_buf.flush();  
                run_num ++;

                // check to see whether need to expand the array
                if (run_num == n_tempfiles)
                {
                    Heapfile[] temp1 = new Heapfile[2*n_tempfiles];
                    for (int i=0; i<n_tempfiles; i++)
                    {
                        temp1[i] = temp_files[i];
                    }
                    temp_files = temp1;
                    n_tempfiles *= 2;

                    int[] temp2 = new int[2*n_runs];
                    for(int j=0; j<n_runs; j++)
                    {
                        temp2[j] = n_tuples[j];
                    }
                    n_tuples = temp2;
                    n_runs *=2;
                }

                try
                {
                    temp_files[run_num] = new Heapfile(null);
                }
                catch (Exception e)
                {
                    throw new BPSortException(e, "Sort.java: create Heapfile failed");
                }

               
                o_buf.init(bufs, _n_pages, tuple_size, temp_files[run_num], false);

                // set the last Elem to be the minimum value for the sort field
                if(order.bpOrder == BPOrder.Ascending)
                {
                    try
                    {
                        MIN_VAL(lastElem, sortFldType);
                    }
                    catch (UnknowAttrType e)
                    {
                        throw new BPSortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
                    }
                    catch (Exception e)
                    {
                        throw new BPSortException(e, "MIN_VAL failed");
                    }
                }
                else
                {
                    try
                    {
                        MAX_VAL(lastElem, sortFldType);
                    }
                    catch (UnknowAttrType e)
                    {
                        throw new BPSortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
                    }
                    catch (Exception e)
                    {
                        throw new BPSortException(e, "MIN_VAL failed");
                    }
                }

                // switch the current heap and the other heap
                BPpnodeSplayPQ tempQ = pcurr_Q;
                pcurr_Q = pother_Q;
                pother_Q = tempQ;
                int tempelems = p_elems_curr_Q;
                p_elems_curr_Q = p_elems_other_Q;
                p_elems_other_Q = tempelems;
            }
            else if (p_elems_curr_Q == 0) 
            {
                while ((p_elems_curr_Q + p_elems_other_Q) < max_elems)
                {
                    try
                    {
                        bp = _am.getnext();
                        if(bp!=null)
                            tuple = bp.getTuplefromBasicPattern();  
                        else
                            tuple = null;
                    }
                    catch (Exception e)
                    {
                        throw new BPSortException(e, "get_next() failed");
                    }

                    if (tuple == null)
                    {
                        break;
                    }
                    cur_node = new BPpnode();
                    cur_node.tuple = new Tuple(tuple); 

                    try
                    {
                        pcurr_Q.enq(cur_node);
                    }
                    catch (UnknowAttrType e)
                    {
                        throw new BPSortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                    }
                    p_elems_curr_Q ++;
                }
            }

            if (p_elems_curr_Q == 0)
            {
                // current queue empty despite our attemps to fill in
                // indicating no more tuples from input
                if (p_elems_other_Q == 0)
                {
                    // other queue is also empty, no more tuples to write out, done
                    break; // of the while(true) loop
                }
                else
                {
                    // generate one more run for all tuples in the other queue
                    // close current run and start next run
                    n_tuples[run_num] = (int) o_buf.flush();  
                    run_num ++;

                    // check to see whether need to expand the array
                    if (run_num == n_tempfiles)
                    {
                        Heapfile[] temp1 = new Heapfile[2*n_tempfiles];
                        for (int i=0; i<n_tempfiles; i++)
                        {
                            temp1[i] = temp_files[i];
                        }
                        temp_files = temp1;
                        n_tempfiles *= 2;

                        int[] temp2 = new int[2*n_runs];
                        for(int j=0; j<n_runs; j++)
                        {
                            temp2[j] = n_tuples[j];
                        }
                        n_tuples = temp2;
                        n_runs *=2;
                    }

                    try
                    {
                        temp_files[run_num] = new Heapfile(null);
                    }
                    catch (Exception e)
                    {
                        throw new BPSortException(e, "Sort.java: create Heapfile failed");
                    }

                    o_buf.init(bufs, _n_pages, tuple_size, temp_files[run_num], false);

                    // set the last Elem to be the minimum value for the sort field
                    if(order.bpOrder == BPOrder.Ascending)
                    {
                        try
                        {
                            MIN_VAL(lastElem, sortFldType);
                        }
                        catch (UnknowAttrType e)
                        {
                            throw new BPSortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
                        }
                        catch (Exception e)
                        {
                            throw new BPSortException(e, "MIN_VAL failed");
                        }
                    }
                    else
                    {
                        try
                        {
                            MAX_VAL(lastElem, sortFldType);
                        }
                        catch (UnknowAttrType e)
                        {
                            throw new BPSortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
                        }
                        catch (Exception e)
                        {
                            throw new BPSortException(e, "MIN_VAL failed");
                        }
                    }

                    // switch the current heap and the other heap
                    BPpnodeSplayPQ tempQ = pcurr_Q;
                    pcurr_Q = pother_Q;
                    pother_Q = tempQ;
                    int tempelems = p_elems_curr_Q;
                    p_elems_curr_Q = p_elems_other_Q;
                    p_elems_other_Q = tempelems;
                }
            } // end of if (p_elems_curr_Q == 0)
        } // end of while (true)

        n_tuples[run_num] = (int) o_buf.flush();
        run_num ++;
        return run_num;
    }

    /**
     * Remove the minimum value among all the runs.
     * @return the minimum tuple removed
     * @exception IOException from lower layers
     * @exception BPSortException something went wrong in the lower layer.
     */
    private Tuple delete_min()
            throws IOException,
            BPSortException,
            Exception
    {
        BPpnode cur_node;                
        Tuple new_tuple, old_tuple;

        cur_node = Q.deq();
        old_tuple = cur_node.tuple;
 
        if (i_buf[cur_node.run_num].empty() != true) {
            new_tuple = new Tuple(tuple_size); 

            try {
                new_tuple.setHdr(n_cols, _in, str_lens);
            }
            catch (Exception e) {
                throw new BPSortException(e, "Sort.java: setHdr() failed");
            }

            new_tuple = i_buf[cur_node.run_num].Get(new_tuple);
            if (new_tuple != null) {
                cur_node.tuple = new_tuple;  
                try {
                    Q.enq(cur_node);
                } catch (UnknowAttrType e) {
                    throw new BPSortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
                } catch (BasicPatternUtilsException e) {
                    throw new BPSortException(e, "Sort.java: BasicPatternUtilsException caught from Q.enq()");
                }
            }
            else {
                throw new BPSortException("********** Wait a minute, I thought input is not empty ***************");
            }

        }

        return old_tuple;
    }

    /**
     * Set lastElem to be the minimum value of the appropriate type
     * @param lastElem the tuple
     * @param sortFldType the sort field type
     * @exception IOException from lower layers
     * @exception UnknowAttrType attrSymbol or attrNull encountered
     */
    private void MIN_VAL(Tuple lastElem, AttrType sortFldType)
            throws IOException,
            FieldNumberOutOfBoundException,
            UnknowAttrType {

        //    short[] s_size = new short[Tuple.max_size]; // need Tuple.java
        //    AttrType[] junk = new AttrType[1];
        //    junk[0] = new AttrType(sortFldType.attrType);

        //    short fld_no = 1;

        switch (sortFldType.attrType) {
            case AttrType.attrInteger:
                //      lastElem.setHdr(fld_no, junk, null);
                lastElem.setIntFld(_sort_fld, Integer.MIN_VALUE);
                lastElem.setIntFld(_sort_fld+1, Integer.MIN_VALUE);
                break;
            case AttrType.attrReal:
                //      lastElem.setHdr(fld-no, junk, null);
                lastElem.setFloFld(_sort_fld, Float.MIN_VALUE);
                break;
            default:
                //System.err.println("error in sort.java");
                throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
        }

        return;
    }

    /**
     * Set lastElem to be the maximum value of the appropriate type
     * @param lastElem the tuple
     * @param sortFldType the sort field type
     * @exception IOException from lower layers
     * @exception UnknowAttrType attrSymbol or attrNull encountered
     */
    private void MAX_VAL(Tuple lastElem, AttrType sortFldType)
            throws IOException,
            FieldNumberOutOfBoundException,
            UnknowAttrType {

        //    short[] s_size = new short[Tuple.max_size]; // need Tuple.java
        //    AttrType[] junk = new AttrType[1];
        //    junk[0] = new AttrType(sortFldType.attrType);

        //    short fld_no = 1;

        switch (sortFldType.attrType) {
            case AttrType.attrInteger:
                //      lastElem.setHdr(fld_no, junk, null);
                lastElem.setIntFld(_sort_fld, Integer.MAX_VALUE);
                lastElem.setIntFld(_sort_fld+1, Integer.MAX_VALUE);
                break;
            case AttrType.attrReal:
                //      lastElem.setHdr(fld_no, junk, null);
                lastElem.setFloFld(_sort_fld, Float.MAX_VALUE);
                break;
            default:
                throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
        }

        return;
    }

    /**
     * Class constructor: set up the sorting basic patterns
     * @param am an iterator for accessing the basicpatterns
     * @param sort_order the sorting order (ASCENDING, DESCENDING)
     * @param sort_fld the field number of the field to sort on ( -1 on confidence )
     * @param n_pages amount of memory (in pages) available for sorting
     * @exception IOException from lower layers
     * @exception BPSortException something went wrong in the lower layer.
     */
    public BPSort(BPFileScan am, BPOrder sort_order, int sort_fld, int n_pages)
            throws IOException, BPSortException
    {
        _am = am;
        order = sort_order;
        _n_pages = n_pages;
        _sort_fld = sort_fld;

        if(sort_fld!=-1)
        {
            _sort_fld = sort_fld*2-1;
        }

        first_time = true;
        max_elems_in_heap = 200;
    }

    /**
     * Returns the next basicpattern in sorted order.
     * Note: You need to copy out the content of the basicpattern, otherwise it
     *       will be overwritten by the next <code>get_next()</code> call.
     * @return the next tuple, null if all basicpatterns exhausted
     * @exception IOException from lower layers
     * @exception BPSortException something went wrong in the lower layer.
     * @exception JoinsException from <code>generate_runs()</code>.
     * @exception UnknowAttrType attribute type unknown
     * @exception LowMemException memory low exception
     * @exception Exception other exceptions
     */
    @Override
    public BasicPattern getnext()
            throws IOException,
            BPSortException,
            UnknowAttrType,
            LowMemException,
            JoinsException,
            Exception
    {
        if (first_time)
        {
            // first get_next call to the sort routine
            first_time = false;

            AttrType sortFldTyp;
            if(_sort_fld<0)		 sortFldTyp = new AttrType(AttrType.attrReal);
            else			 sortFldTyp = new AttrType(AttrType.attrInteger);
            // generate runs
            Nruns = generate_runs(max_elems_in_heap, sortFldTyp);

            // setup state to perform merge of runs.
            // Open input buffers for all the input file
            setup_for_merge(tuple_size, Nruns);
        }

        if (Q.empty())
        {
            // no more tuples available
            return null;
        }

        output_tuple = delete_min();
        if (output_tuple != null)
        {
            op_buf.tupleCopy(output_tuple);
            BasicPattern bpnew = new BasicPattern(output_tuple);
            return bpnew;
        }
        else
        {
            return null;
        }
    }

    /**
     * Cleaning up, including releasing buffer pages from the buffer pool
     * and removing temporary files from the database.
     * @exception IOException from lower layers
     */
    public void close() throws IOException
    {
        if (!closeFlag)
        {

            try
            {
                _am.close();
            }
            catch (Exception e)
            {
                try {
                    throw new BPSortException(e, "Sort.java: error in closing iterator.");
                } catch (BPSortException e1) {
                    e1.printStackTrace();
                }
            }

            if (useBM)
            {
                try
                {
                    free_buffer_pages(_n_pages, bufs_pids);
                }
                catch (Exception e)
                {
                    try {
                        throw new BPSortException(e, "Sort.java: BUFmgr error");
                    } catch (BPSortException e1) {
                        e1.printStackTrace();
                    }
                }
                for (int i=0; i<_n_pages; i++) bufs_pids[i].pid = INVALID_PAGE;
            }

            for (int i = 0; i<temp_files.length; i++)
            {
                if (temp_files[i] != null)
                {
                    try
                    {
                        temp_files[i].deleteFile();
                    }
                    catch (Exception e)
                    {
                        try {
                            throw new BPSortException(e, "Sort.java: Heapfile error");
                        } catch (BPSortException e1) {
                            e1.printStackTrace();
                        }
                    }
                    temp_files[i] = null;
                }
            }
            closeFlag = true;
        }
    }
}