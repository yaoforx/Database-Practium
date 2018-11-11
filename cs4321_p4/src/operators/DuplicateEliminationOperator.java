package operators;

import util.Tuple;

import java.io.PrintStream;

/**
 * DuplicateEliminationOperator assumes its child is a SortOperator. since the input from its child is in sorted order,
 * it reads the tuples from the child and only outputs non-duplicates. this operator is needed when there is a DISTINCT
 * keyword in the query.
 *
 * @authors Yao Xiao, Kyle Johnson, Valerie Tan
 */
public class DuplicateEliminationOperator extends Operator {
    SortOperator child;
    Tuple prev = null;
    Tuple cur = null;

    /**
     * calls reset() on the child of this DuplicateEliminationOperator
     */
    @Override
    public void reset() {
        child.reset();
    }

    /**
     * @return the next tuple of the child of this DuplicateEliminationOperator
     */
    @Override
    public Tuple getNextTuple() {

        while ((cur = child.getNextTuple()) != null) {
            if(cur.equals(prev)) {
               // prev = cur;
                continue;
            }
            prev = cur;
            return cur;
        }
        return null;
    }

    @Override
    public String print() {
        return "DupElim\n";
    }

    @Override
    public void printTree(PrintStream ps, int lv) {
        ps.print(print());
        child.printTree(ps,lv + 1);
    }


    /**
     * constructs a DuplicateEliminationOperator
     *
     * @param child  the child of this DuplicateEliminationOperator. will always be a SortOperator due to the
     *               structure of the operator tree
     */
    public DuplicateEliminationOperator(Operator child) {

        this.child = (SortOperator) child;

    }
}
