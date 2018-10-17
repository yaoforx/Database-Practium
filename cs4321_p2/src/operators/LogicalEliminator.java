package operators;

import visitors.PhysicalPlanBuilder;
/**
 * Constructs Logical Elimination Operator
 */
public class LogicalEliminator extends LogicalOperator {
    public LogicalOperator child;

    public void accept(PhysicalPlanBuilder phyplan) { phyplan.visit(this); }

    public LogicalEliminator(LogicalOperator child) {
        this.child = child;
    }
}
