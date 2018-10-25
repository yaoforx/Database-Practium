package operators;

import net.sf.jsqlparser.statement.select.OrderByElement;
import util.Tuple;

import java.util.*;

public class SortInMemory extends SortOperator {
    List<Integer> sort = new ArrayList<>();
    List<Tuple> inputs = new ArrayList<>();
    private int cur = 0;


    @Override
    public void reset(int pageNum, int index) {
        cur = index;
    }
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
        child.reset();
        Collections.sort(inputs,new Comparator<Tuple>(){

            /**
             * lambda function compares two tuples
             *
             * @param t1 the first tuple
             * @param t2 the second tuple
             * @return
             */
            public int compare(Tuple t1,Tuple t2){

                HashSet<Integer> orderby = new HashSet<>(sort);
                //  System.out.println(sort.toString());

                for(int i = 0; i < sort.size(); i++) {
                    int v1 = t1.getValue(sort.get(i));
                    int v2 = t2.getValue(sort.get(i));
                    int comp = Integer.compare(v1,v2);
                    if(comp !=0) return comp;
                }
                //If you are here means ORDER BY has not broken ties
                //then sort based on original remaining schemas
                for(int i = 0; i < t1.getSize(); i++) {
                    if(orderby.contains(i)) continue;
                    int v1 = t1.getValue(i);
                    int v2 = t2.getValue(i);
                    int comp = Integer.compare(v1,v2);
                    if(comp !=0) return comp;
                }
                return 0;

            }});
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
        if(cur >= inputs.size()) return null;
        return inputs.get(cur++);
    }
}
