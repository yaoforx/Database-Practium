package operators;

import net.sf.jsqlparser.expression.Expression;

import util.Tuple;


import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Sort Merge Join Operator implements Sort Merge Join algorhims
 * to combine two qualified tuples
 * @author Yao Xiao
 */

public class SortMergeJoin extends JoinOperator {


    private List<Integer> leftOrder; // OrderBy in left tuple
    private List<Integer> rightOrder;// OrderBy in right tuple

    int position ;  // the index of the first tuple in the current partition
    int curIndex ;  // the index of the current tuple of outer
    int curPage = 0;


    Tuple leftTuple;
    Tuple rightTuple;
    private joinCmp cp = null; // comparator for comparing two tuples in specific order


    /**
     * Constructor for SMJ
     * @param exp expression to evaluate
     * @param left left operator
     * @param right right operator
     * @param leftorder left operator's order
     * @param rightorder right operator's order
     */
    public SortMergeJoin(Expression exp, Operator left, Operator right, List<Integer> leftorder, List<Integer> rightorder)
    {

        super(exp,left,right);
        this.leftOrder = leftorder;
        this.rightOrder = rightorder;
        leftTuple= left.getNextTuple();
        rightTuple = right.getNextTuple();
        cp = new joinCmp(leftOrder,rightOrder);
        position = 0;
        curIndex= 0;

    }

    /**
     * Override method for comparing two tuples
     */

    public class joinCmp implements Comparator<Tuple>{
        List<Integer> leftOrders = null; // the order of attributes in left table
        List<Integer> rightOrders = null;// the order of attributes in right table
        @Override
        public int compare(Tuple left, Tuple right) {
            for( int i = 0; i< leftOrders.size();i++){
                int leftVal = left.getValue(leftOrder.get(i));
                int rightVal = right.getValue(rightOrder.get(i));
                int cmp = Integer.compare(leftVal, rightVal);
                if(cmp != 0) return cmp;
            }

            return 0;
        }

        public joinCmp(List<Integer> leftOrders, List<Integer> rightOrders){
            this.leftOrders = leftOrders;
            this.rightOrders = rightOrders;
        }
    }


    /**
     * getNextTuple method evaluates inner and outer tuple
     * @return a qualified joined tuple
     */
    @Override
    public Tuple getNextTuple() {
        Tuple res = null;
        while(leftTuple !=null && rightTuple !=null){

            if (cp.compare(leftTuple, rightTuple) < 0) {
                leftTuple = left.getNextTuple();
                continue;

            }

            if(cp.compare(leftTuple, rightTuple) > 0) {
                rightTuple = right.getNextTuple();
                curIndex++;
                position = curIndex;
                continue;

            }
            if(exp == null || satisfy(leftTuple, rightTuple)){
                res = joinTuple(leftTuple,rightTuple);

            }
            rightTuple = right.getNextTuple();
            curIndex++;
            if (rightTuple== null || cp.compare(leftTuple,rightTuple) != 0) {
                leftTuple= left.getNextTuple();
                ((SortOperator)right).reset(position);
                curIndex = position;
                rightTuple = right.getNextTuple();
            }

            if (res != null) return res;
        }

        return null;
    }

    @Override
    public void reset(int index) {

    }
    @Override
    public boolean satisfy(Tuple l, Tuple r) {

        return super.satisfy(l,r);
    }


    private Tuple joinTuple(Tuple l, Tuple r) {

        List<Integer> newschema= new ArrayList<>();
        for(int i = 0; i < l.getSize(); i++) {
            newschema.add(l.getValue(i));
        }
        for(int i = 0; i < r.getSize(); i++) {
            newschema.add(r.getValue(i));
        }
        return new Tuple(newschema);


    }



}
