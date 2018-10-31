package visitors;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import util.*;

//TODO: comments for JoinVisitors
/**
 * Join visitors combine two tuples
 * and filter them based on condition
 */
public class JoinVisitors extends AbstractVisitors {
    private Tuple tp1;
    private Tuple tp2;
    public List<String> schema1;
    public List<String> schema2;

    /**
     *
     * @param s1
     * @param s2
     */
    public JoinVisitors(List<String> s1, List<String> s2){
        this.schema1 = s1;
        this.schema2 = s2;

    }

    /**
     *
     * @param t1
     * @param t2
     */
    public void setTuple(Tuple t1, Tuple t2) {
        this.tp1 = t1;
        this.tp2 = t2;
    }

    /**
     *
     * @param schema
     * @param t
     * @param column
     * @return
     */
    private long getValue(List<String> schema, Tuple t, Column column) {

        int idx = schema.indexOf(column.toString());

        long val = Integer.MAX_VALUE;
        if(idx != -1) {
            val = t.getValue(idx);


        }
        return val;

    }

    /**
     *
     * @param column
     */
    @Override
    public void visit(Column column) {
        //String tableName = column.getTable().getName();

        //List<String> schema
            long val = getValue(schema1, tp1, column);

            if (val == Integer.MAX_VALUE)
                val = getValue(schema2, tp2, column);

            value = val;
    }
}
