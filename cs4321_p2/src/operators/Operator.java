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

    /**
     * repeatedly calls getNextTuple() until the next tuple is null and writes each tuple to PrintStream ps
     *
     * @param ps the PrintStream where the tuples will be printed
     */
    public void dump(PrintStream ps) {
        Tuple t;
        while((t = this.getNextTuple()) != null) {
            t.dump(ps);
        }

    }
    public void dump(TupleWriter writer) throws IOException {
        Tuple t;
        while((t = this.getNextTuple()) != null) {
            writer.write(t);
        }
    }

}
