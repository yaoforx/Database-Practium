package operators;

import net.sf.jsqlparser.statement.select.OrderByElement;
import util.DBCatalog;
import util.Tuple;

import java.beans.Expression;
import java.util.*;


/**
 * SortOperator sorts tuples based on the ORDER BY clause in the query, then by the original schema
 *
 * @authors Yao Xiao, Kyle Johnson, Valerie Tan
 */
public abstract class SortOperator extends Operator {
    List<Integer> sort;
    List<String> sortInEle;

    public Operator child;

    public externalCmp compare = null;


    public class externalCmp implements Comparator<Tuple> {
        List<Integer> ties;
        HashSet<Integer> orderby;

        @Override
        public int compare(Tuple t1, Tuple t2) {
           // HashSet<Integer> orderby = new HashSet<>(sort);
            //  System.out.println(sort.toString());

            for(int i = 0; i < orderby.size(); i++) {
                int v1 = t1.getValue(ties.get(i));
                int v2 = t2.getValue(ties.get(i));
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
        }
        public externalCmp(List<Integer> ties) {
            this.ties = ties;
            this.orderby = new HashSet<>(ties);
        }
    }



    public abstract void reset(int index);


    /**
     * constructs a SortOperator
     *
     * @param child the child of this SortOperator
     * @param orders
     */
    public SortOperator(Operator child, List<?> orders) {


        sort = new ArrayList<>();
        sortInEle  = new ArrayList<>();
        this.schema = child.schema;
        this.child = child;

        if (!orders.isEmpty()) {
            if (orders.get(0) instanceof OrderByElement) {
                for (Object obj : orders) {
                    OrderByElement obe = (OrderByElement) obj;
                    sortInEle.add(obe.toString());
                    this.sort.add(getColIdx(
                            obe, this.child.schema));
                }
            }
            else if (orders.get(0) instanceof Integer) {
                this.sort = (List<Integer>) orders;
                for(Integer i :this.sort)
                    sortInEle.add(getElementByIndex(i));
            }
            else
                throw new IllegalArgumentException();
        }
        compare = new externalCmp(sort);

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
    public String getElementByIndex(int idx){

        return child.schema.get(idx);

    }



}
