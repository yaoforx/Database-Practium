package operators;
import net.sf.jsqlparser.statement.select.SelectItem;
import util.*;

import net.sf.jsqlparser.expression.Expression;
import java.util.*;
import visitors.SelectVisitors;

import java.io.PrintStream;

/**
 * SelectOperator selects tuples by filtering them based on the optional WHERE clause of the query
 *
 * @authors Yao Xiao, Kyle Johnson, Valerie Tan
 */
public class SelectOperator extends Operator {
    Expression expression = null;
    SelectVisitors sv = null;
    ScanOperator child;
    private List<SelectItem> items;

    /**
     * constructs a SelectOperator
     *
     * @param expression the where condition of the Select statement. will be null if no where condition
     * @param scan       the child of the SelectOperator being constructed
     */
    public SelectOperator(Expression expression, ScanOperator scan) {
        child = new ScanOperator(scan.tb);
        schema = child.schema;
        this.expression = expression;
        sv = new SelectVisitors(child.schema);


    }

    /**
     * @return the child of this SelectOperator
     * @see    ScanOperator
     */
    public ScanOperator getChild(){
        return child;
    }

    /**
     * calls reset() on the child to reset its BufferReader to a new one at the beginning of the table
     */
    @Override
    public void reset() {
        child.reset();
    }

    /**
     * @return the child's next tuple if it is not null
     * @see    Tuple
     */
    @Override
    public Tuple getNextTuple() {
        Tuple tp;
        while((tp = child.getNextTuple()) != null) {
            if(satisfy(expression,sv,tp)) {
               // System.out.println("selecting " + tp.toString());
                return tp;
            }
        }


        return null;
    }

    /**
     * evaluates whether a tuple satisfies a where condition
     *
     * @param ex an Expression representing the where condition that the tuple may or may not satisfy
     * @param sv the visitor that checks whether tp satisfies ex
     * @param tp the tuple being checked for whether it satisfies the where condition or not
     * @return   true or false depending on whether tp satisfied ex
     * @see      Expression
     * @see      SelectVisitors
     * @see      Tuple
     */
    public boolean satisfy(Expression ex, SelectVisitors sv, Tuple tp){
        sv.setTuple(tp);
        ex.accept(sv);

        return sv.getCondition();
    }

    /**
     * repeatedly calls getNextTuple() until the next tuple is null and writes each tuple to PrintStream ps
     *
     * @param ps the PrintStream where the tuples will be printed
     */
    @Override
    public void dump(PrintStream ps) {
        Tuple tp;
        while((tp = getNextTuple()) != null) {
            tp.dump(ps);
        }

    }
}
