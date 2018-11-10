package operators;
import util.*;
import net.sf.jsqlparser.expression.Expression;
import unionfind.UnionFind;
import util.Tuple;
import util.Util;
import visitors.PhysicalPlanBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class LogicalMassJoin extends LogicalOperator{
    private Expression expression;
    public UnionFind unionFind;
    public HashMap<String, Expression> conditions;
    public List<String> tables;
    public List<LogicalOperator> children;



    public LogicalMassJoin(List<String> froms, List<LogicalOperator> jointables, HashMap<String, Expression> conditions, UnionFind uf){
        this.unionFind = uf;
        this.conditions = conditions;
        this.tables = froms;
        this.children = jointables;
        List<Expression> list = new ArrayList<>();
        for(String s : conditions.keySet()) list.addAll(Util.getAndExpressions(conditions.get(s)));
        if(!list.isEmpty()) this.expression = Util.genAnds(list);


    }


    @Override
    public void accept(PhysicalPlanBuilder paramPhysicalPlanBuilder) {

    }
}