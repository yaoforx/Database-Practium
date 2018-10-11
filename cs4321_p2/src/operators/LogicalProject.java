package operators;

import visitors.PhysicalPlanBuilder;


public class LogicalProject extends LogicalOperator {
    public java.util.List<net.sf.jsqlparser.statement.select.SelectItem> selectItems;
    public LogicalOperator child;

    public void accept(PhysicalPlanBuilder phyplan) { phyplan.visit(this); }

    public LogicalProject(java.util.List<net.sf.jsqlparser.statement.select.SelectItem> items, LogicalOperator child) {
        selectItems = items;
        this.child = child;
    }
}

