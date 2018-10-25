package operators;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import util.DBCatalog;
import util.Tuple;
import visitors.JoinVisitors;
import visitors.SelectVisitors;

import java.util.ArrayList;
import java.util.List;

public class SortMergeJoin extends JoinOperator {

    public Expression expression;
    public JoinVisitors jv;
    public Tuple leftTuple;
    public Tuple rightTuple;
    private List<Integer> leftOrder;
    private List<Integer> rightOrder;
    private int index;
    private int pageNum;

    @Override
    public void reset() {
        super.reset();
    }

    public SortMergeJoin(Expression exp, Operator left, Operator right, List<Integer> leftorder, List<Integer> rightorder)
    {

        super(exp,left,right);
        this.expression = exp;
        this.leftOrder = leftorder;
        this.rightOrder = rightorder;
        this.jv = new JoinVisitors(left.schema, right.schema);

        leftTuple = left.getNextTuple();
        rightTuple = right.getNextTuple();
        index = 0;




    }

    public int compare(Tuple tpLeft, Tuple tpRight, List<Integer> leftOrder, List<Integer>rightOrder){

        for (int i = 0; i < leftOrder.size(); i++){
            int vLeft = tpLeft.getValue(i);
            int vRight = tpRight.getValue(i);
            if (vLeft < vRight){
                return -1;
            }else if (vLeft > vRight){
                return 1;
            }
        }
        return 0;
    }

    @Override
    public Tuple getNextTuple() {

     while(leftTuple != null && rightTuple != null) {
            while(compare(leftTuple, rightTuple, leftOrder, rightOrder) == -1) {
                leftTuple = left.getNextTuple();
                if(leftTuple == null) return null;

            }
            while(compare(leftTuple, rightTuple, leftOrder, rightOrder)== 1) {
                rightTuple = right.getNextTuple();
                index = rightTuple.getIdxInPage();
                pageNum = rightTuple.getPageNum();
            }
            Tuple res = null;
            if(expression == null || satisfy(leftTuple,rightTuple)) {
                res = joinTuple(leftTuple,rightTuple);

            }
            rightTuple = right.getNextTuple();
            index = rightTuple.getIdxInPage();
            pageNum = rightTuple.getPageNum();
            if(rightTuple == null || compare(leftTuple,rightTuple,leftOrder, rightOrder) != 0) {
                leftTuple = left.getNextTuple();
                ((SortOperator) right).reset(pageNum,index);
                index = rightTuple.getIdxInPage();
                pageNum = rightTuple.getPageNum();
                rightTuple = right.getNextTuple();
            }
            if(res != null) return res;
     }
     return null;
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
