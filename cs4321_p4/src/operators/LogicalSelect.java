package operators;

import java.io.IOException;
import java.io.PrintStream;

import net.sf.jsqlparser.expression.Expression;
import visitors.PhysicalPlanBuilder;
/**
 * Constructs Logical Select Operator
 */
public class LogicalSelect extends LogicalOperator
{
    public Expression exp;
    public LogicalScan scan;

    public void accept(PhysicalPlanBuilder phyplan) {
        phyplan.visit(this);
    }

    public LogicalSelect(Expression expression, LogicalOperator scan) {
        exp = expression;
        this.scan = ((LogicalScan)scan);
    }
    @Override
    public String print() {
        return String.format("Select[%s]",
                ((exp == null) ? "null" : exp.toString())) + "\n";
    }

    @Override
    public void printTree(PrintStream ps, int lv) {
        printIndent(ps, lv);
        ps.print(print());
        scan.printTree(ps, lv + 1);
    }

}
