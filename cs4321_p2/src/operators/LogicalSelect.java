package operators;

import java.io.IOException;
import net.sf.jsqlparser.expression.Expression;
import visitors.PhysicalPlanBuilder;

public class LogicalSelect
        extends LogicalOperator
{
    public Expression exp;
    public LogicalScan scan;

    public void accept(PhysicalPlanBuilder phyplan) {

            phyplan.visit(this);

    }

    public LogicalSelect(Expression expression, LogicalOperator scan) { exp = expression;
        this.scan = ((LogicalScan)scan);
    }
}
