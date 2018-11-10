package operators;

import java.io.PrintStream;
import java.util.List;
import net.sf.jsqlparser.statement.select.OrderByElement;
import visitors.PhysicalPlanBuilder;
import visitors.*;

/**
 * Constructs Logical Sort Operator
 */

public class LogicalSort extends LogicalOperator
{
    public List<OrderByElement> order;
    public LogicalOperator child;
    @Override
    public String print() {
        return String.format("Sort%s",
                ((order == null) ? "[null]" : order.toString()));
    }

    @Override
    public void printTree(PrintStream ps, int lv) {

    }

    public void accept(PhysicalPlanBuilder phyplan) { phyplan.visit(this); }

    public LogicalSort(List<OrderByElement> order, LogicalOperator child) {
        this.child = child;
        this.order = order;
    }
}
