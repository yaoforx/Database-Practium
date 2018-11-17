package optimal;

import com.oracle.tools.packager.Log;
import net.sf.jsqlparser.expression.Expression;
import operators.LogicalOperator;
import operators.LogicalSort;
import operators.Operator;
import util.Tuple;
import util.Util;
import visitors.PhysicalPlanBuilder;


import java.util.*;

public class OptimalJoin {


      	// the cost of the optimal sub problem.
        private HashMap<List<String>, Double> plans;	// the optimal plan.
        private int size;
        public HashSet<String> tables = new HashSet<>();


        public OptimalJoin() {
            plans = new HashMap<>();
            size = 0;

        }
        public void decomposeJoinAndCalculate(List<String> joins) {
                List<String> tableInjoins =joins;
                size  = joins.size();

                DPcalculate(tableInjoins);

        }
        private Double DPcalculate(List<String> subtables){
            if(subtables.size() == 0) return 1.0;
            if (plans.containsKey(subtables)) {
                // System.out.println("tables calculated: " + subtables.toString() + ", cost: " + plans.get(subtables));

                return plans.get(subtables);
            }

            if(subtables.size() == 1) {


                double value =  OptimalSelect.vvalues.getVforTable(subtables.get(0));
               // System.out.println("plan1: " + subtables.toString() + ", cost: " + value);
              plans.put(subtables, OptimalSelect.vvalues.getVforTable(subtables.get(0)));
              return value;
            }
            if(subtables.size() == 2) {
                // System.out.println("plan: " + plan.toString() + ", cost: " + plans.get(plan));

                double value = OptimalSelect.vvalues.getVforTable(subtables.get(0)) * OptimalSelect.vvalues.getVforTable(subtables.get(1));
                plans.put(subtables, value);
              //  System.out.println("plan2: " + subtables.toString() + ", cost: " + value);
                return value;
            }
            double value = 1;

            for(int i = 0; i < subtables.size(); i++) {
                List<String> left = subtables.subList(0, i);
                List<String> right = subtables.subList(i + 1, subtables.size());
                List<String> outer = new ArrayList<>();
                outer.add(subtables.get(i));
                value = DPcalculate(outer)*DPcalculate(left)*DPcalculate(right);
                outer.addAll(left);
                outer.addAll(right);
                if(outer.size() > size) break;
              //  System.out.println("plan3: " + outer.toString() + ", cost: " + value);
                plans.put(outer, value);
            }

            return value;

        }




    public List<String> getOptimalJoin(){
            List<String> res = new ArrayList<>();
            double cost = Double.MAX_VALUE;
            for(List<String> plan : plans.keySet()) {
                    System.out.println("plan: " + plan.toString() + ", cost: " + plans.get(plan));
                    if(plans.get(plan) <= cost) {
                        res = plan;

                }
            }
            return res;
    }



}
