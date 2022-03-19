package iterator;

import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import quadrupleheap.*;

/**
 * A structure describing a quadruple
 * include a run number and the quadruple
 */

public class Quadruplepnode {
    /** which run does this quadruple belong */
    public int     run_num;

    /** the quadruple reference */
    public Quadruple   quadruple;

    /**
     * class constructor, sets <code>run_num</code> to 0 and <code>quadruple</code>
     * to null.
     */
    public Quadruplepnode()
    {
        run_num = 0;  // this may need to be changed
        quadruple = null;
    }

    /**
     * class constructor, sets <code>run_num</code> and <code>quadruple</code>.
     * @param runNum the run number
     * @param t      the quadruple
     */
    public Quadruplepnode(int runNum, Quadruple t)
    {
        run_num = runNum;
        quadruple = t;
    }
}



