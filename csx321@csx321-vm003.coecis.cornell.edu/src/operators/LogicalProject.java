package operators;

import net.sf.jsqlparser.statement.select.SelectItem;
import visitors.PhysicalPlanBuilder;

import java.io.PrintStream;
import java.util.List;
/**
 * Constructs Logical Projection Operator
 */

public class LogicalProject extends LogicalOperator {
    public List<SelectItem> selectItems;
    public LogicalOperator child;

    @Override
    public String print() {
        return String.format("Project%s",
                ((selectItems == null) ? "[null]" : selectItems.toString())) + "\n";
    }

    @Override
    public void printTree(PrintStream ps, int lv) {
        printIndent(ps, lv);
        ps.print(print());
        child.printTree(ps, lv + 1);
    }

    public void accept(PhysicalPlanBuilder phyplan) {
        phyplan.visit(this);
    }

    public LogicalProject(List<SelectItem> items, LogicalOperator child) {
        selectItems = items;
        this.child = child;
    }
}

