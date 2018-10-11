package operators;

import visitors.PhysicalPlanBuilder;

public abstract class LogicalOperator
{
    public LogicalOperator() {}

    public abstract void accept(PhysicalPlanBuilder paramPhysicalPlanBuilder);
}
