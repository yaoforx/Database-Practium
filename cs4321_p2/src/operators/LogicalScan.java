package operators;

import util.Table;

public class LogicalScan extends LogicalOperator
{
    public Table table;

    public void accept(visitors.PhysicalPlanBuilder phyplan)
    {
        try {
            phyplan.visit(this);
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
        }
    }

    public LogicalScan(Table tb) { table = tb; }
}
