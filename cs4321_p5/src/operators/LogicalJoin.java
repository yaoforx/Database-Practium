package operators;

import net.sf.jsqlparser.expression.Expression;
import visitors.PhysicalPlanBuilder;

import java.io.PrintStream;

/**
 * Constructs Logical Join Operator
 */
public class LogicalJoin extends LogicalOperator
{
    public LogicalOperator left;
    public LogicalOperator right;
    public Expression expression;

    @Override
    public String print() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void printTree(PrintStream ps, int lv) { }

    public void accept(PhysicalPlanBuilder phyplan) { phyplan.visit(this); }

    public LogicalJoin(Expression exp, LogicalOperator left, LogicalOperator right) {
        this.left = left;
        this.right = right;

        expression = exp;
    }
}
