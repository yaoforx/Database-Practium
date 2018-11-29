package operators;

import util.DBCatalog;
import util.Table;
import visitors.*;

import java.io.IOException;
import java.io.PrintStream;

/**
 * Constructs Logical Scan Operator
 */
public class LogicalScan extends LogicalOperator {
    public Table table;



    public void accept(PhysicalPlanBuilder phyplan) {

            phyplan.visit(this);

    }
    @Override
    public String print() {
        return String.format("Leaf[%s]", DBCatalog.alias.get(table.tableName));
    }

    @Override
    public void printTree(PrintStream ps, int lv) {
        printIndent(ps, lv);
        ps.println(print());
    }


    public LogicalScan(Table tb) {
        this.table = tb;
    }
}
