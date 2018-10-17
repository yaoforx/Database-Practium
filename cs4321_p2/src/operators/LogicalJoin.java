package operators;

import net.sf.jsqlparser.expression.Expression;
import visitors.PhysicalPlanBuilder;


public class LogicalJoin
        extends LogicalOperator
{
    public LogicalOperator left;
    public LogicalOperator right;
    public net.sf.jsqlparser.expression.Expression expression;

    public void accept(PhysicalPlanBuilder phyplan) { phyplan.visit(this); }

    public LogicalJoin(Expression exp, LogicalOperator left, LogicalOperator right) {
        this.left = left;
        this.right = right;
        expression = exp;
    }
}
