package operators;


import jnio.TupleWriter;
import util.Tuple;

import java.io.IOException;
import java.util.*;
import java.io.PrintStream;
/**
 * abstract Operator class that all other operators will extend
 *
 * @authors Yao Xiao, Kyle Johnson, Valerie Tan
 */
public abstract class Operator {
    public List<String> schema = null;
    public abstract void reset();

    public abstract Tuple getNextTuple();
   // public abstract void reset(int index);

    /**
     * repeatedly calls getNextTuple() until the next tuple is null and writes each tuple to PrintStream ps
     *
     * @param ps the PrintStream where the tuples will be printed
     */
    public void dump(PrintStream ps) {
        Tuple t;
        while((t = getNextTuple()) != null) {
            t.dump(ps);
        }

    }
    public void dump(TupleWriter writer) throws IOException {
        Tuple t;

        while((t = getNextTuple()) != null) {
            writer.write(t);
        }
    }

    // print out the indentation.
    protected static void printIndent(PrintStream ps, int lv) {
        while (lv-- > 0)
            ps.print('-');
    }

    /**
     * Prints this operator
     * @return
     */
    public abstract String print();

    /**
     * Print the plan
     * @param ps
     * @param lv
     */
    public abstract void printTree(PrintStream ps, int lv);

}
