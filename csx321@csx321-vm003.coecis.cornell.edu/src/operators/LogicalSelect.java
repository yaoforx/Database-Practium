package operators;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import btree.Btree;
import net.sf.jsqlparser.expression.Expression;
import util.DBCatalog;
import util.Util;
import util.indexInfo;
import visitors.PhysicalPlanBuilder;

import static visitors.PhysicalPlanBuilder.optimalSelect;

/**
 * Constructs Logical Select Operator
 */
public class LogicalSelect extends LogicalOperator
{
    public Expression exp;
    public LogicalScan scan;
    public boolean idxSelect;
    public indexInfo info;

    public void accept(PhysicalPlanBuilder phyplan) {
        phyplan.visit(this);
    }

    public LogicalSelect(Expression expression, LogicalOperator scan) {
        exp = expression;
        this.scan = ((LogicalScan)scan);
        if(DBCatalog.config.idxSelect) {
            String tabName = this.scan.table.tableName;
            tabName = Util.getFullTableName(tabName);
            info = optimalSelect.calculateAndChoose(tabName, exp);
            idxSelect = (info != null);

        }
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
