package operators;

import net.sf.jsqlparser.expression.Expression;
import util.DBCatalog;
import util.Tuple;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Block Nested Loop Join Operator
 * @author Yao Xiao
 * @author Kyle Johnson
 */

public class BlockNestedJoin extends JoinOperator {
    private List<Tuple> outerBlock;
    private int tupleInBlock;
    private int outerIdx;
    private int size = 4096;
    private int pageInBlock;
    private int posInBlock;
    public Tuple leftTuple = null;
    public Tuple rightTuple = null;




    public BlockNestedJoin(Expression exp, Operator left, Operator right) {
        super(exp, left,right);
        pageInBlock = DBCatalog.config.joinPage;
        posInBlock = 0;
        leftTuple = left.getNextTuple();
        rightTuple = right.getNextTuple();
        if(leftTuple != null) {
            tupleInBlock = pageInBlock * size/leftTuple.getSize()/4;
            outerBlock = new ArrayList<>(tupleInBlock);
            outerBlock.add(leftTuple);

            outerIdx = 1;
            readAt(1);

        }


    }

    /**
     * Helper function to read outer Relation into Block
     * starting from i
     *
     * @param i
     * @return true if read successfully
     */

    private boolean readAt(int i) {

        int read = i;
        while(read < tupleInBlock && (leftTuple = left.getNextTuple()) != null) {
            outerBlock.add(leftTuple);
            read++;
            outerIdx++;
        }
        return read > i;
    }
    @Override
    public void reset(int idx) {


    }
    private boolean readOuter() {
        outerBlock.clear();
        outerIdx = 0;
        return readAt(0);

    }
    @Override
    public void reset() {
        super.reset();
        rightTuple = right.getNextTuple();
        posInBlock = 0;
        readOuter();

    }

    @Override
    public Tuple getNextTuple() {
       Tuple res = null;
       while(true) {
           if(rightTuple != null) {
               if(posInBlock < outerIdx) {
                   if(satisfy(outerBlock.get(posInBlock), rightTuple))
                        res  = joinTuple(outerBlock.get(posInBlock), rightTuple);
                   posInBlock++;
               } else {
                   posInBlock = 0;
                   rightTuple = right.getNextTuple();
               }
           } else {

               right.reset();
               rightTuple = right.getNextTuple();
               if(!readOuter()) {
                   return null;
               }
           }
           if(res != null) return res;

       }

    }

    @Override
    public String print() {
        String expression = (exp != null) ? exp.toString() : "";
        return String.format("BNLJ[" + expression + "]");
    }

    @Override
    public void printTree(PrintStream ps, int lv) {

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
