package visitors;

import java.io.FileReader;
import java.io.IOException;

import operators.*;
import util.Configure;
import util.DBCatalog;

import java.io.BufferedReader;
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
            root = new SortOperator(root, logSort.order);
        else
            root = new ExternalSort(root, logSort.order);
    }

    public void visit(LogicalJoin logJoin) {
        Operator[] child = new operators.Operator[2];
        logJoin.left.accept(this);
        child[0] = root;
        logJoin.right.accept(this);
        child[1] = root;
            root = new operators.JoinOperator(logJoin.expression, child[0], child[1]);
    }
}
