package operators;

import net.sf.jsqlparser.statement.select.OrderByElement;
import util.Tuple;

import java.util.*;

public class SortInMemory extends SortOperator {

    List<Tuple> inputs = new ArrayList<>();
    private int cur = 0;



    @Override
    public void reset() {
        cur = 0;
    }

    /**
     * constructs a SortOperator
     *
     * @param child the child of this SortOperator
     * @param ties
     */
    public SortInMemory(Operator child, List<?> ties) {
        super(child, ties);
        Tuple tp = null;

        while ((tp = child.getNextTuple()) != null)
            inputs.add(tp);

        Collections.sort(inputs,compare);

    }

    /**
     *
     * @param element The element to order by
     * @param schema the schema this element is using
     * @return the column index
     */
    static public int getColIdx(OrderByElement element, List<String> schema) {
        int idx = schema.indexOf(element);

        if (idx != -1) {
            return idx;
        }

        else {

            for (int i = 0; i < schema.size(); i++) {
                String col = schema.get(i);
                col = col.split("\\.")[1];

                if (col.equals(element.toString().split("\\.")[1])) {
                    return i;
                }
            }

        }
        return -1;
    }

    @Override
    public Tuple getNextTuple() {
        if(cur >= this.inputs.size()) return null;
        return inputs.get(cur++);
    }

    @Override
    public void reset(int index) {
        cur = index;
    }
}
