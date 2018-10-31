package operators;

import visitors.PhysicalPlanBuilder;
/**
 * Constructs abstract class for Logical Operator
 */
public abstract class LogicalOperator
{
    public LogicalOperator() {}

    public abstract void accept(PhysicalPlanBuilder paramPhysicalPlanBuilder);
}
