package visitors;

import java.io.FileReader;
import java.io.IOException;

import net.sf.jsqlparser.expression.Expression;
import operators.*;
import util.Configure;
import util.DBCatalog;
import util.Util;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;

/**
 * PhysicalPlanBuilder is to construct Physical Operator tree
 * based on Configuration file and SQL query
 * @author Yao Xiao
 */
public class PhysicalPlanBuilder {
    private Operator root = null;


    public PhysicalPlanBuilder() {

    }


    public operators.Operator getRoot() {

        return root;
    }

    public void visit(LogicalScan logScan) {

        root = new ScanOperator(logScan.table);
    }

    public void visit(LogicalSelect logSelect)  {
        logSelect.scan.accept(this);
        root = new SelectOperator(logSelect.exp, (ScanOperator)root);
    }

    public void visit(LogicalProject logProj) {
        logProj.child.accept(this);
        root = new ProjectOperator(logProj.selectItems, root);
    }

    public void visit(LogicalEliminator logElim) {
        logElim.child.accept(this);
        root = new DuplicateEliminationOperator(root);
    }

    public void visit(LogicalSort logSort) {
        logSort.child.accept(this);
        if (DBCatalog.config.externalSort == 0)
            root = new SortInMemory(root, logSort.order);
        else
            root = new ExternalSort(root, logSort.order);
    }
    public void visit(LogicalJoin logJoin) {
        Operator[] child = new operators.Operator[2];
        root = null;
        logJoin.left.accept(this);
        child[0] = root;
        root = null;
        logJoin.right.accept(this);
        child[1] = root;
        if(DBCatalog.config.SMJ == 1) {
            List<Integer> outIdxs = new ArrayList<Integer>();
            List<Integer> inIdxs = new ArrayList<Integer>();
            Expression newExp = Util.procJoinConds(
                    logJoin.expression, child[0].schema,
                    child[1].schema, outIdxs, inIdxs);

            if (outIdxs.size() != inIdxs.size())
                throw new IllegalArgumentException();

            if (!outIdxs.isEmpty()) {
                logJoin.expression = newExp;
                if (DBCatalog.config.externalSort == 0) {
                    child[0] = new SortInMemory(child[0], outIdxs);
                    child[1] = new SortInMemory(child[1], inIdxs);
                }
                else {
                    child[0] = new ExternalSort(child[0], outIdxs);
                    child[1] = new ExternalSort(child[1], inIdxs);
                }
                root = new SortMergeJoin(logJoin.expression, child[0], child[1], outIdxs, inIdxs);

            }


        } else if(DBCatalog.config.TNLJ == 1) {
            root = new TupleNestedJoin(logJoin.expression, child[0], child[1]);
        } else if(DBCatalog.config.BNLJ == 1) {
            root = new BlockNestedJoin(logJoin.expression, child[0], child[1]);
        }


    }
}
