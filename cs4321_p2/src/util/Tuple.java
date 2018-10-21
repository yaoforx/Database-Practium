package util;
import jnio.TupleReader;

import java.io.IOException;
import java.util.*;
import java.io.PrintStream;

/**
 * Tuple represents a tuple or array of integers in a table and also stores the tuple size
 *
 * @authors Yao Xiao, Kyle Johnson, Valerie Tan
 */
public class Tuple{
    public List<Integer> tuple = new ArrayList<>();
    private int cols;
    public TupleReader tupleReader = null;

    /**
     * constructs a Tuple. calculates the number of columns using the size() method of List
     *
     * @param tuple the list of integers to be stored as the Tuple
     */
    public Tuple(List<Integer> tuple) {
        this.tuple = tuple;
        this.cols = tuple.size();

    }

    /**
     * @return the number of columns in this tuple
     */
    public int getSize() {
        return this.cols;
    }

    /**
     * @param i the column index to be retrieved. an i of 0 refers to the first column
     * @return  the value of the tuple at index i
     */
    public int getValue(int i) {
        return tuple.get(i);
    }

    /**
     * @return the hash value of this tuple
     */
    @Override
    public int hashCode() {
        return tuple.toString().hashCode();
    }

    /**
     * override of the equals() function in order to effectively compare two tuples
     *
     * @param obj the other object that this tuple is being compared to
     * @return true if this tuple is structurally equivalent to obj
     */
    @Override
    public boolean equals(Object obj) {
        if(obj  == null) return false;

        Tuple tp = (Tuple) obj;
        if(this.getSize() != tp.getSize()) return false;
        for(int i = 0; i < this.getSize(); i++) {
            if(this.tuple.get(i).intValue() != tp.tuple.get(i).intValue()) {
                return false;
            }
        }
        return true;
    }

    /**
     * prints this tuple to PrintStream s
     *
     * @param s the PrintStream to which the tuple will be printed
     */
    public void dump(PrintStream s) {
        Tuple tp = new Tuple(tuple);
        String str = tp.toString()+ "\n";
        s.append(str);

    }

    /**
     * override of the toString() method because we do not want the address of the string
     *
     * @return the tuple in String form
     */
    @Override
    public String toString() {
        if (cols < 1) return "";
        // StringBuilder sb = new StringBuilder();
        String sb = "";
        int i = 0;
        while (i < tuple.size()) {
            sb += tuple.get(i).toString();
            if(i == tuple.size() - 1) break;
            sb +=",";
            i++;

        }
        return sb;
    }
}

