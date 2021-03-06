package operators;

import util.Table;
import visitors.*;

import java.io.IOException;
/**
 * Constructs Logical Scan Operator
 */
public class LogicalScan extends LogicalOperator {
    public Table table;

    public void accept(PhysicalPlanBuilder phyplan) {

            phyplan.visit(this);

    }

    public LogicalScan(Table tb) {
        this.table = tb;
    }
}
