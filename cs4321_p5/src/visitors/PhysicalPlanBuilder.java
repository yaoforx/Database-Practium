package visitors;

import java.io.File;


import btree.Btree;
import net.sf.jsqlparser.expression.Expression;
import operators.*;

import optimal.OptimalJoin;
import optimal.OptimalSelect;
import optimal.Vvalues;
import util.DBCatalog;
import util.Selector;
import util.Util;
import util.indexInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;


/**
 * PhysicalPlanBuilder is to construct Physical Operator tree
 * based on Configuration file and SQL query
 * @author Yao Xiao
 */
public class PhysicalPlanBuilder {
    private Operator root = null;
    public static OptimalSelect optimalSelect = new OptimalSelect();
    public static OptimalJoin optimalJoin = new OptimalJoin();



    public PhysicalPlanBuilder() {




    }
    public operators.Operator getRoot() {

        return root;
    }

    public void visit(LogicalScan logScan) {

        root = new ScanOperator(logScan.table);
    }

    public void visit(LogicalSelect logSelect)  {

        Operator scanner = null;
        if(DBCatalog.config.idxSelect) {

            if(logSelect.idxSelect) {
                if(logSelect.info != null) {
                    Integer[] range
                            = Util.getLowAndHeigh(logSelect.info.indexCol, logSelect.exp);
                    String idxPath = DBCatalog.indexdir + logSelect.info.tab + '.' + logSelect.info.indexCol;
                    File idxFile = new File(idxPath);
                    Btree btree = DBCatalog.idxConfig.loaders.get(logSelect.info.tab).getBtree();
                    scanner = new IndexScanOperator(logSelect.scan.table, range[0], range[1], btree, idxFile);
                }
            }

        }

        if(scanner == null) {

            scanner = new ScanOperator(logSelect.scan.table);
        }
        root = new SelectOperator(logSelect.exp, scanner);

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
    public void visit(LogicalMassJoin logMass) {

        List<Operator> physical = new ArrayList<>();


        List<LogicalOperator> res = new ArrayList<>();
        String ss = logMass.tables.toString();
       // optimalJoin.decomposeJoinAndCalculate(logMass.tables);
      //  List<String> optimalTable = optimalJoin.getOptimalJoin();
      //  String s = optimalTable.toString();




       // logMass.tables = optimalTable;

        Operator[] child = new operators.Operator[logMass.children.size()];
        logMass.children.get(0).accept(this);
        child[0] = root;
        String ct = child[0].schema.toString();
        for(int i = 1; i < logMass.children.size(); i++) {
            logMass.children.get(i).accept(this);
            child[i] = root;
            String ctt = child[i].schema.toString();
            if (DBCatalog.config.SMJ == 1) {
                List<Integer> outIdxs = new ArrayList<Integer>();
                List<Integer> inIdxs = new ArrayList<Integer>();

                boolean hasNoneEual = Util.checkEqual(logMass.expression);
                Expression newExp = Util.procJoinConds(
                        logMass.expression, child[i - 1].schema,
                        child[i].schema, outIdxs, inIdxs);

                if (outIdxs.size() != inIdxs.size())
                    throw new IllegalArgumentException();

                if (!outIdxs.isEmpty() && !hasNoneEual) {

                    if (DBCatalog.config.externalSort == 0) {
                        child[i - 1] = new SortInMemory(child[i - 1], outIdxs);
                        child[i] = new SortInMemory(child[i], inIdxs);
                    } else {
                        child[i - 1] = new ExternalSort(child[i - 1], outIdxs);
                        child[i] = new ExternalSort(child[i], inIdxs);
                    }
                    root = new SortMergeJoin(newExp, child[i - 1], child[i], outIdxs, inIdxs);

                } else {
                    root = new BlockNestedJoin(newExp, child[i - 1], child[i]);
                }

            }
        }

    }
    public void visit(LogicalJoin logJoin) {
        if(logJoin.expression == null) {
            System.out.print("sss");
        }


        Operator[] child = new operators.Operator[2];

        logJoin.left.accept(this);

        child[0] = root;

        logJoin.right.accept(this);
        child[1] = root;
        if(DBCatalog.config.SMJ == 1) {
            List<Integer> outIdxs = new ArrayList<Integer>();
            List<Integer> inIdxs = new ArrayList<Integer>();

            boolean hasNoneEual = Util.checkEqual(logJoin.expression);
            Expression newExp = Util.procJoinConds(
                    logJoin.expression, child[0].schema,
                    child[1].schema, outIdxs, inIdxs);

            if (outIdxs.size() != inIdxs.size())
                throw new IllegalArgumentException();

            if (!hasNoneEual) {
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

            } else {
                root = new BlockNestedJoin(logJoin.expression, child[0], child[1]);
            }


        } else if(DBCatalog.config.TNLJ == 1) {
            root = new TupleNestedJoin(logJoin.expression, child[0], child[1]);
        } else if(DBCatalog.config.BNLJ == 1) {
            root = new BlockNestedJoin(logJoin.expression, child[0], child[1]);
        } else {
            root = new TupleNestedJoin(logJoin.expression, child[0], child[1]);
        }


    }



}
