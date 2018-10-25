package operators;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import util.Tuple;

/**
 * ProjectOperator filters tuples based on the fields specified after the SELECT statement in the query.
 * for a SELECT * statement, all columns of the tuples are passed through and the ProjectOperator does nothing.
 * otherwise, the ProjectOperator filters tuples based on the SelectExpressionItem
 *
 * @authors Yao Xiao, Kyle Johnson, Valerie Tan
 */
public class ProjectOperator extends Operator{
    protected Operator projectChild = null;
    protected SelectOperator selectChild = null;
    protected ScanOperator scanChild = null;
    protected Operator joinChild = null;

    /**
     * constructs a ProjectOperator
     *
     * @param selectItems a list of the items in a SELECT query
     * @param scan        child of this ProjectOperator. can be a SelectOperator, ScanOperator, or JoinOperator
     */
    public ProjectOperator(List<SelectItem> selectItems, Operator scan) {

        //If this is all column
        //Do not do anything


        if(!selectItems.isEmpty() && (selectItems.get(0) instanceof AllColumns)) {
            if(scan instanceof SelectOperator) {
                selectChild =(SelectOperator)scan;
            } else if(scan instanceof ScanOperator) {
                scanChild = (ScanOperator) scan;
            } else
                joinChild=  scan;
            schema = scan.schema;


            return;
        }
        if(scan instanceof SelectOperator) {
            selectChild =(SelectOperator)scan;
        } else if(scan instanceof ScanOperator) {
            scanChild = (ScanOperator) scan;
        } else
            joinChild = scan;


        List<String> proSchema = new ArrayList<>();
        List<String> childSchema = scan.schema;
        HashSet<String> allTabCols = new HashSet<String>();


        for(SelectItem item : selectItems) {
            Column column = (Column)((SelectExpressionItem) item).getExpression();
            String tableName = column.getTable().getName();
           // System.out.println("table name is " + tableName);

            String colName  = column.getColumnName();
            //If the expression is Sailors.A, Sailors.B
//            if (item instanceof AllTableColumns)
//                allTabCols.add(item.toString().split(".")[0]);
            if(tableName != null) {
                proSchema.add(tableName + "." + colName);
            } else {
                //If the expression is SELECT A, B From Sailors
                for(String s : childSchema) {
                    if(colName.equals(s.split(".")[1])) {
                        proSchema.add(s);
                        break;
                    }
                }

            }
        }

        if(allTabCols.isEmpty()) {
            schema = proSchema;
        }
        else {
            for (String tabCol : childSchema) {
                String tab = tabCol.split(".")[0];
                if (allTabCols.contains(tab) || proSchema.contains(tabCol));
                schema.add(tabCol);
            }
        }

    }

    /**
     * calls reset() on the child of this ProjectOperator
     */
    @Override
    public void reset() {
        projectChild = selectChild;
        if(projectChild == null) projectChild = scanChild;
        if(projectChild == null) projectChild = joinChild;
        projectChild.reset();
    }

    /**
     * @return the child's next tuple
     */
    @Override
    public Tuple getNextTuple() {
        projectChild = selectChild;
        if (projectChild == null) projectChild = scanChild;
        if (projectChild == null) projectChild = joinChild;
        Tuple tp = projectChild.getNextTuple();
        if (tp == null) {
            return null;
        }
        List<Integer> columns = new ArrayList<>();

        for (String sch : schema) {
            columns.add(getValue(tp, sch, projectChild.schema).intValue());
        }




        return new Tuple(columns);
    }

    @Override
    public void reset(int index) {

    }

    /**
     *
     * @param tp Tuple to find desired value
     * @param sche schema for this tuple
     * @param schemas  schema for this projection
     * @return corresponding value to the column
     */
    public static Long getValue(Tuple tp, String sche, List<String> schemas) {
        int idx = schemas.indexOf(sche);

        if (idx != -1) {
            return (long) tp.getValue(idx);
        }
        //if it is using column reference
        //"SELECT * FROM Table WHERE Table.A = 1;"
        else {

            for (int i = 0; i < schemas.size(); i++) {
                //here getting A from previous example
                String col = schemas.get(i);
                col = col.split("\\.")[1];

                if (col.equals(sche.split("\\.")[1])) {
                    return (long)tp.getValue(i);
                }
            }

        }
        return null;
    }
}