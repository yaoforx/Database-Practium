package operators;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.io.FileReader;

import jnio.TupleReader;
import util.*;

/**
 * ScanOperator fetches content from a table
 *
 * @authors Yao Xiao, Kyle Johnson, Valerie Tan
 */
public class ScanOperator extends Operator {

    Table tb;
    private TupleReader lines;
    //private BufferedReader lines;

    /**
     * constructs a ScanOperator
     *
     * @param tb the table from which the ScanOperator will be retrieving data
     * @see      Table
     */
    public ScanOperator(Table tb) {

        this.tb = tb;
        lines =  DBCatalog.tableReader(tb.tableName);
        schema = new ArrayList<>();
        for(String col : tb.getSchema()) {
            schema.add(tb.tableName + "." + col);
        }
    }

    /**
     * @return the table associated with this ScanOperator
     * @see    Table
     */
    public Table getTable() {
        return tb;
    }

    /**
     * @return the next tuple to be read from the table
     * @see    Tuple
     */
    public Tuple getNextTuple() {
        Tuple tp;
        try {
            // String line = lines.read();
            //if(line == null) return null;
            //String[] cols = line.split(",");
            //List<Integer> elements = new ArrayList<>();
            //for(int i = 0; i < cols.length;i++) {
            //  elements.add(Integer.valueOf(cols[i]));

            return lines.read();
       }catch(IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * resets the BufferReader to a new one at the beginning of the table
     */
    public void reset() {
        try {
            lines.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        lines = DBCatalog.tableReader(tb.tableName);
    }

    /**
     * repeatedly calls getNextTuple() until the next tuple is null and writes each tuple to PrintStream ps
     *
     * @param s the PrintStream where the tuples will be printed
     */
    @Override
    public void dump(PrintStream s) {
        Tuple tuple;
        while((tuple = getNextTuple()) != null) {
            tuple.dump(s);

        }
    }
}
