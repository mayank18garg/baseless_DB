package BPIterator;

import java.io.IOException;
import java.util.ArrayList;

import bufmgr.PageNotReadException;
import heap.*;
// import diskmgr.*;
import global.*;
import index.IndexException;
import iterator.*;
import labelheap.LabelHeapfile;
import quadrupleheap.Quadruple;
import quadrupleheap.QuadrupleHeapfile;
import quadrupleheap.TScan;
import labelheap.*;

public class BPTripleJoin extends BPIterator {

    int amt_of_mem;
    int num_left_nodes;
    BPIterator left_iter;
    int BPJoinNodePosition;
    int JoinOnSubjectorObject;
    String RightSubjectFilter;
    String RightPredicateFilter;
    String RightObjectFilter;
    double RightConfidenceFilter;
    int [] LeftOutNodePosition;
    int OutputRightSubject;
    int OutputRightObject;

    private   boolean    done;         // Is the join 
	private boolean	  get_from_outer;

    //// 
    QuadrupleHeapfile Quadruple_HF;
    private TScan inner;
	private  BasicPattern  outer_tuple;
	private Quadruple inner_quadruple;


    public BPTripleJoin(int amt_of_mem, int num_left_nodes, BPIterator left_itr, int BPJoinNodePosition, 
            int JoinOnSubjectorObject, String RightSubjectFilter, String RightPredicateFilter, String RightObjectFilter, 
            double RightConfidenceFilter, int [] LeftOutNodePositions, int OutputRightSubject, int OutputRightObject){

        this.amt_of_mem = amt_of_mem;
        this.num_left_nodes = num_left_nodes;
        this.left_iter = left_itr;
        this.BPJoinNodePosition = BPJoinNodePosition;
        this.JoinOnSubjectorObject = JoinOnSubjectorObject;
        this.RightSubjectFilter = RightSubjectFilter;
        this.RightPredicateFilter = RightPredicateFilter;
        this.RightObjectFilter = RightObjectFilter;
        this.RightConfidenceFilter = RightConfidenceFilter;
        this.LeftOutNodePosition = LeftOutNodePositions;
        this.OutputRightSubject = OutputRightSubject;
        this.OutputRightObject = OutputRightObject;

        get_from_outer = true;
        done = false;
        inner = null;
        outer_tuple = null;
        inner_quadruple = null;
    }




    @Override
    public BasicPattern getnext() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
            InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
            LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {

        if(done) return null;
        do{
            if(get_from_outer == true){
                get_from_outer = false;
                if(inner!=null){
                    inner.closescan();
                    inner = null;
                }
                try{
                    Quadruple_HF = SystemDefs.JavabaseDB.getQuadrupleHandle();
                    inner = new TScan(Quadruple_HF);
                }
                catch(Exception e){
                    throw new NestedLoopException(e, "openScan failed");
                }
                if((outer_tuple=left_iter.getnext()) == null){
                    done = true;
                    if(inner!=null){
                        inner.closescan();
                        inner = null;
                    }
                    return null;
                }
            }

            QID qid = new QID();
            LabelHeapfile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
            LabelHeapfile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();
            while((inner_quadruple = inner.getNext(qid)) != null){
                Label subject = Entity_HF.getLabel(inner_quadruple.getSubjecqid().returnLID());
                Label predicate = Predicate_HF.getLabel(inner_quadruple.getPredicateID().returnLID());
                Label object = Entity_HF.getLabel(inner_quadruple.getObjecqid().returnLID());
                double confidence = inner_quadruple.getConfidence();
                boolean result = true;
                if(RightSubjectFilter.compareToIgnoreCase("null") != 0)
                {
                    result = result & (RightSubjectFilter.compareTo(subject.getLabel()) == 0);	
                }
                if(RightObjectFilter.compareToIgnoreCase("null") != 0)
                {
                    result = result & (RightObjectFilter.compareTo(object.getLabel()) == 0 );	
                }
                if(RightPredicateFilter.compareToIgnoreCase("null") != 0)
                {
                    result = result & (RightPredicateFilter.compareTo(predicate.getLabel()) == 0 );	
                }
                if(RightConfidenceFilter != 0)
                {
                    result = result & (confidence >= RightConfidenceFilter);
                }
                if(result){
                    ArrayList<EID> arrEID = new ArrayList<EID>();
                    EID eid_o = outer_tuple.getNodeID(BPJoinNodePosition).returnLID().returnEID();
                    EID eid_i;
                    if(JoinOnSubjectorObject == 0) eid_i = inner_quadruple.getSubjecqid();
                    else eid_i = inner_quadruple.getObjecqid();
                    double min_conf = 0.0;
                    if(confidence <= outer_tuple.getConfidence())
                        min_conf = confidence;
                    else
                        min_conf = outer_tuple.getConfidence();
                    if(eid_i.equals(eid_o)){
                        BasicPattern bp = new BasicPattern();

                    
                        for(int i = 1; i <= outer_tuple.noOfFlds() - 1;i++)	
                        {
                            for(int j = 0; j < LeftOutNodePosition.length; j++)
                            {
                                if(LeftOutNodePosition[j] == i)
                                {
                                    arrEID.add(outer_tuple.getNodeID(i).returnLID().returnEID());
                                    break;
                                }
                            }
                        }
                        if(OutputRightSubject == 1 && JoinOnSubjectorObject == 0){
                            boolean isPresent = false;
                            for(int k = 0; k < LeftOutNodePosition.length; k++)
                            {
                                if(LeftOutNodePosition[k] == BPJoinNodePosition)
                                {
                                    isPresent = true;
                                    break;
                                }
                            }
                            if(!isPresent) arrEID.add(inner_quadruple.getSubjecqid());
                        }
                        else if(OutputRightSubject == 1 && JoinOnSubjectorObject == 1){
                            arrEID.add(inner_quadruple.getSubjecqid());
                        }
                        if(OutputRightObject == 1 && JoinOnSubjectorObject == 1){
                            boolean isPresent = false;
                            for(int k = 0; k < LeftOutNodePosition.length; k++)
                            {
                                if(LeftOutNodePosition[k] == BPJoinNodePosition)
                                {
                                    isPresent = true;
                                    break;
                                }
                            }
                            if(!isPresent)
                                arrEID.add(inner_quadruple.getObjecqid());
                        }
                        else if(OutputRightObject == 1 && JoinOnSubjectorObject == 0){
                            arrEID.add(inner_quadruple.getObjecqid());
                        }
                        if(arrEID.size() != 0){
                            // todo: fill bp
                            bp.setHdr((short)(arrEID.size()));
                            for(int i=0; i < arrEID.size(); i++){
                                bp.setEIDIntFld(i+1, arrEID.get(i));
                            }
                            bp.setConfidence(min_conf);
                            return bp;
                        }
                    }
                }
            }
            get_from_outer = true;

        }while(true);
        
        // return null;
    }

    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {
        if (!closeFlag) {

			try {
				if(inner!=null)
				{
				    inner.closescan();
				}
				left_iter.close();
			}catch (Exception e) {
				System.out.println("NestedLoopsJoin.java: error in closing iterator."+e);
			}
			closeFlag = true;
		}

        
    }
    
}
