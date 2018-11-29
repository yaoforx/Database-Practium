package visitors;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import util.*;
import java.util.*;

//TODO: comments for SelectVisitors
/**
 * The class will take as input a tuple
 * and recursively walk the expression to
 * evaluate it to true or false on that tuple.
 *
 * @author Yao Xiao
 */
public class SelectVisitors extends AbstractVisitors {
    public Tuple t;
    public List<String> schema;

    /**
     *
     * @param schema
     */
    public SelectVisitors(List<String> schema){
        this.schema = schema;

    }

    public void setTuple(Tuple t) {
        this.t = t;
    }

    /**
     * Extract value in column with reference
     * @param column
     */
    @Override
    public void visit(Column column) {
        int idx = schema.indexOf(column.toString());

        if(idx != -1) {
            value = t.getValue(idx);

            return;
        }
        //if it is using column reference
        //"SELECT * FROM Table WHERE Table.A = 1;"
        else {

            for(int i = 0; i< schema.size(); i++) {
                //here getting A from previous example
                String col = schema.get(i);
                col = col.split("\\.")[1];
                if(col.equals(column.toString().split("\\.")[1])) {
                    value = t.getValue(i);
                    return;
                }
            }
            value = (Long)null;
        }


    }

}
