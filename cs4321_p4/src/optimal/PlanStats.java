package optimal;

import operators.Operator;

import java.util.List;

public class PlanStats {
    private double cost;
    private List<Operator> plans;
    public PlanStats(){
        cost = Double.MAX_VALUE;
        plans = null;
    }
}
