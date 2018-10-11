package visitors;

import java.io.FileReader;
import java.io.IOException;

import operators.*;
import java.io.BufferedReader;
public class PhysicalPlanBuilder
{
    private operators.Operator root = null;
    private BufferedReader reader;
    private int joinMethod = -1;
    private int sortMethod = -1;
    private int joinPage = -1;
    private int sortPage = -1;

    private void setUp() throws IOException { int[][] info = new int[2][2];
        for (int i = 0; i < 2; i++) {
            String[] temp = reader.readLine().split(" ");
            info[i][0] = Integer.parseInt(temp[0]);
            if (temp.length == 2) {
                info[i][1] = Integer.parseInt(temp[1]);
            } else
                info[i][1] = -1;
        }
        joinMethod = info[0][0];
        sortMethod = info[1][0];
        if (info[0][1] != -1) {
            joinPage = info[0][1];
        }
        if (info[1][1] != -1)
            sortPage = info[1][1];
    }

    public PhysicalPlanBuilder(String input) throws IOException {
        try { reader = new java.io.BufferedReader(new FileReader(input));
        } catch (IOException e) {
            e.printStackTrace();
        }
        setUp();
    }

    public operators.Operator getRoot() {
        return root;
    }

    public void visit(LogicalScan logScan) throws IOException { root = new ScanOperator(logScan.table); }

    public void visit(LogicalSelect logSelect) throws IOException {
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
        if (sortMethod == 0)
            root = new SortOperator(root, logSort.order);
    }

    public void visit(LogicalJoin logJoin) {
        operators.Operator[] child = new operators.Operator[2];
        logJoin.left.accept(this);
        child[0] = root;
        root = null;
        logJoin.right.accept(this);
        child[1] = root;
        if (joinMethod == 0) {
            root = new operators.JoinOperator(logJoin.expression, child[0], child[1]);
        }
    }
}
