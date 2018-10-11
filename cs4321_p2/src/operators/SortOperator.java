package operators;

import net.sf.jsqlparser.statement.select.OrderByElement;
import util.Tuple;

import java.util.*;


/**
 * SortOperator sorts tuples based on the ORDER BY clause in the query, then by the original schema
 *
 * @authors Yao Xiao, Kyle Johnson, Valerie Tan
 */
public class SortOperator extends Operator {
    List<Integer> sort;
    List<Tuple> inputs;
    private int cur = 0;

    /**
     *  reset List<Tuple> idx to zero
     */
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
    public SortOperator(Operator child, List<OrderByElement> ties) {
        Tuple tp;
        inputs = new ArrayList<>();
        sort = new ArrayList<>();
        while((tp = child.getNextTuple()) != null) {
            inputs.add(tp);
        }
        if(!ties.isEmpty()) {
            for (OrderByElement element : ties) {
                sort.add(getColIdx(element, child.schema));
            }
        }
        //Override lambda function for comparison
        //work on java 8
        //TODO: need to check with TA to see if we are running java 8
    //    System.out.println(sort.toString());
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
    private int getColIdx(OrderByElement element, List<String> schema) {
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

    /**
     *
     * @return next tuple in List
     */
    @Override
    public Tuple getNextTuple() {
        if(cur >= inputs.size()) return null;
        return inputs.get(cur++);
    }
}
