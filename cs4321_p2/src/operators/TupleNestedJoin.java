package operators;

import net.sf.jsqlparser.expression.Expression;
import util.Tuple;

import java.util.ArrayList;
import java.util.List;

public class TupleNestedJoin extends JoinOperator{

    @Override
    public void reset() {
        super.reset();
    }

    public TupleNestedJoin(Expression exp, Operator left, Operator right) {
        super(exp, left, right);
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
    @Override
    public boolean satisfy(Tuple l, Tuple r) {
        return super.satisfy(l, r);
    }

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
}
