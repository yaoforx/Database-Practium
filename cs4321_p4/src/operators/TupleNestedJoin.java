package operators;

import net.sf.jsqlparser.expression.Expression;
import util.Tuple;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Tuple Nested Loop Join
 */
public class TupleNestedJoin extends JoinOperator{
    Tuple l = null;
    Tuple r = null;

    @Override
    void reset(int idx) {

    }

    @Override
    public void reset() {
        super.reset();
    }

    public TupleNestedJoin(Expression exp, Operator left, Operator right) {
        super(exp, left, right);
        l = left.getNextTuple();
        r = right.getNextTuple();

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

    /**
     * Satisfy function return true if this tuple can be selected
     * @param l the left tuple
     * @param r the right tuple
     * @return
     */
    @Override
    public boolean satisfy(Tuple l, Tuple r) {
        return super.satisfy(l, r);
    }

    /**
     * Tuple Nested Loop Join
     * @return new tuple
     */
    public Tuple getNextTuple(){
        Tuple res = null;


        while(l != null) {

            while(r != null) {
                if(exp == null) {
                    res = joinTuple(l,r);
                } else if(satisfy(l,r)) {
                    res = joinTuple(l,r);
                }
                if(r != null)
                    r = right.getNextTuple();
                if(res != null)  {

                    return res;
                }
            }
            l = left.getNextTuple();
            right.reset();

            r = right.getNextTuple();
        }

        return null;
    }

    @Override
    public String print() {
        String expression = (exp != null) ? exp.toString() : "";
        return String.format("TNLJ[" + expression + "]") + "\n";
    }

    @Override
    public void printTree(PrintStream ps, int lv) {

    }


}
