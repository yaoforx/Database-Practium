package operators;

import java.util.List;
import net.sf.jsqlparser.statement.select.OrderByElement;
import visitors.PhysicalPlanBuilder;
import visitors.*;



public class LogicalSort extends LogicalOperator
{
    public List<OrderByElement> order;
    public LogicalOperator child;

    public void accept(PhysicalPlanBuilder phyplan) { phyplan.visit(this); }

    public LogicalSort(List<OrderByElement> order, LogicalOperator child) {
        this.child = child;
        this.order = order;
    }
}
