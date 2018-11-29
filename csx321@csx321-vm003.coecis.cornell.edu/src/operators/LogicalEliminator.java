package operators;

import visitors.PhysicalPlanBuilder;

import java.io.PrintStream;

/**
 * Constructs Logical Elimination Operator
 */
public class LogicalEliminator extends LogicalOperator {
    public LogicalOperator child;

    @Override
    public String print() {
        return "DupElim\n";
    }

    @Override
    public void printTree(PrintStream ps, int lv) {

        ps.print(print());
        child.printTree(ps,lv + 1);
    }

    public void accept(PhysicalPlanBuilder phyplan) { phyplan.visit(this); }

    public LogicalEliminator(LogicalOperator child) {
        this.child = child;
    }
}
