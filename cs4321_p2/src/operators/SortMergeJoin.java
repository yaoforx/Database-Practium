package operators;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import util.DBCatalog;
import util.Tuple;
import visitors.JoinVisitors;
import visitors.PhysicalPlanBuilder;
import visitors.SelectVisitors;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
//
//public class SortMergeJoin extends JoinOperator {
//
//    public Expression expression;
//    public JoinVisitors jv;
//    public Tuple leftTuple;
//    public Tuple rightTuple;
//    private List<Integer> leftOrder;
//    private List<Integer> rightOrder;
//    private int index;
//    private int pageNum;
//    private TupleComp compare = null;
//
//
//
//    public SortMergeJoin(Expression exp, Operator left, Operator right, List<Integer> leftorder, List<Integer> rightorder)
//    {
//
//        super(exp,left,right);
//        this.expression = exp;
//        this.leftOrder = leftorder;
//        this.rightOrder = rightorder;
//        this.jv = new JoinVisitors(left.schema, right.schema);
//
//
//        leftTuple = left.getNextTuple();
//        rightTuple = right.getNextTuple();
//        index = 0;
//        compare =  new TupleComp(leftorder,rightorder);
//
//
//
//
//    }
//
//    public class TupleComp implements Comparator<Tuple> {
//        List<Integer> leftOrders = null; // the order of attributes in left table
//        List<Integer> rightOrders = null;// the order of attributes in right table
//        @Override
//        public int compare(Tuple left, Tuple right) {
//            for( int i = 0; i< leftOrders.size();i++){
//                int leftVal = left.getValue(leftOrders.get(i));
//                int rightVal = right.getValue(rightOrders.get(i));
//                int cmp = Integer.compare(leftVal, rightVal);
//                if(cmp != 0) return cmp;
//            }
//
//            return 0;
//        }
//
//        public TupleComp(List<Integer> leftOrders, List<Integer> rightOrders){
//            this.leftOrders = leftOrders;
//            this.rightOrders = rightOrders;
//        }
//    }
//
//
//    @Override
//    public Tuple getNextTuple() {
//
//     while(leftTuple != null && rightTuple != null) {
//            if(compare.compare(leftTuple, rightTuple) < 0) {
//                leftTuple = left.getNextTuple();
//                continue;
//
//            }
//           if(compare.compare(leftTuple, rightTuple) > 0) {
//                rightTuple = right.getNextTuple();
//                index = rightTuple.getIdxInPage();
//                pageNum = rightTuple.getPageNum();
//                continue;
//            }
//            Tuple res = null;
//            if(expression == null || satisfy(leftTuple,rightTuple)) {
//                res = joinTuple(leftTuple,rightTuple);
//
//            }
//            rightTuple = right.getNextTuple();
//            if(rightTuple != null) {
//                index = rightTuple.getIdxInPage();
//                pageNum = rightTuple.getPageNum();
//            }
//            if(rightTuple == null || compare.compare(leftTuple,rightTuple) != 0) {
//                leftTuple = left.getNextTuple();
//                ((SortOperator)right).reset(pageNum,index);
//                if(rightTuple != null) {
//                    index = rightTuple.getIdxInPage();
//                    pageNum = rightTuple.getPageNum();
//                }
//                rightTuple = right.getNextTuple();
//            }
//            if(res != null) {
//              //  System.out.println(res.toString());
//                return res;
//            }
//     }
//     return null;
//    }
//
//    private Tuple joinTuple(Tuple l, Tuple r) {
//
//        List<Integer> newschema= new ArrayList<>();
//        for(int i = 0; i < l.getSize(); i++) {
//            newschema.add(l.getValue(i));
//        }
//        for(int i = 0; i < r.getSize(); i++) {
//            newschema.add(r.getValue(i));
//        }
//        return new Tuple(newschema);
//
//
//    }
//
//
//
//}
public class SortMergeJoin extends JoinOperator {


    private List<Integer> leftOrder;
    private List<Integer> rightOrder;

    int partitionIndex = 0;  // the index of the first tuple in the current partition
    int curRightIndex = 0;  // the index of the current tuple of left table

    private JoinVisitors jv;
    Tuple leftTp;
    Tuple rightTp;
    externalCmp cp = null;




    public SortMergeJoin(Expression exp, Operator left, Operator right, List<Integer> leftorder, List<Integer> rightorder)
    {

        super(exp,left,right);
        // System.out.println("left order" + leftorder.toString() + " right order" + rightorder);

        this.leftOrder = leftorder;
        this.rightOrder = rightorder;

        this.jv = new JoinVisitors(left.schema,right.schema);
        leftTp = left.getNextTuple();
        rightTp = right.getNextTuple();
        cp = new externalCmp(leftOrder,rightOrder);
        partitionIndex = 0;
        curRightIndex = 0;






    }

    public class externalCmp implements Comparator<Tuple>{
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

        public externalCmp(List<Integer> leftOrders, List<Integer> rightOrders){
            this.leftOrders = leftOrders;
            this.rightOrders = rightOrders;
        }
    }




    @Override
    public Tuple getNextTuple() {
        Tuple rst = null;

        while(leftTp !=null && rightTp !=null){
            if (cp.compare(leftTp, rightTp) < 0) {
                leftTp = left.getNextTuple();
                if(leftTp != null)
                    //    System.out.println(leftTp.toString());
                    continue;
            }

            if (cp.compare(leftTp, rightTp) > 0) {
                rightTp = right.getNextTuple();
                curRightIndex++;
                partitionIndex = curRightIndex;
                continue;
            }

            if(exp == null || satisfy(leftTp, rightTp)){
                //  System.out.println(leftTp.toString() + "," + rightTp.toString());
                rst = joinTuple(leftTp,rightTp);

            }
            rightTp = right.getNextTuple();
            curRightIndex++;
            if (rightTp == null ||
                    cp.compare(leftTp, rightTp) != 0) {

                leftTp = left.getNextTuple();
                if(leftTp != null)
                    //  System.out.println(leftTp.toString());
                    // System.out.println(leftTp);
                    ((SortOperator) right).reset(partitionIndex);
                curRightIndex = partitionIndex;
                rightTp = right.getNextTuple();
            }

            if (rst != null) return rst;
        }

        return null;
    }

    @Override
    public void reset(int index) {

    }

    @Override
    public boolean satisfy(Tuple l, Tuple r) {

        jv.setTuple(l,r);
        exp.accept(jv);
        return jv.getCondition();
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
