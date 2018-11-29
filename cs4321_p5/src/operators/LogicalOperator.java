package operators;

import visitors.PhysicalPlanBuilder;

import java.io.PrintStream;

/**
 * Constructs abstract class for Logical Operator
 */
public abstract class LogicalOperator
{
    public LogicalOperator() {}
    public abstract String print();
    protected static void printIndent(PrintStream ps, int lv) {
        while (lv-- > 0)
            ps.print('-');
    }
    public abstract void printTree(PrintStream ps, int lv);
    public abstract void accept(PhysicalPlanBuilder paramPhysicalPlanBuilder);
}
