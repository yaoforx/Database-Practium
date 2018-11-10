package optimal;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import util.DBCatalog;
import util.Table;
import util.TableStat;

import java.util.HashMap;

public class Vvalues {
    static HashMap<String, Integer> Vvalues = null;
    static HashMap<String, TableStat> tablestats = null;
    public static void getVvalues(String[] baseTable, Expression[] selecTable, Expression[] joinTable) {
        tablestats = DBCatalog.tablestats;
        Vvalues = new HashMap<>();
        for(String tb : baseTable) {
            String tabName = tb.split("\\,")[0];
            String attName = tb.split("\\.")[1];
            int[] range = tablestats.get(tabName).getRange(attName);

            Vvalues.put(tabName, range[1] - range[0] + 1);
        }

        for(Expression exp : selecTable) {
            Column left = null;
            Integer right = null;
            if(exp instanceof GreaterThan) {
                if(((GreaterThan) exp).getLeftExpression() instanceof Column) {
                    left = (Column) ((GreaterThan) exp).getLeftExpression();
                    LongValue value = (LongValue) ((GreaterThan) exp).getRightExpression();
                    right = (int) value.getValue();
                } else {
                    left = (Column) ((GreaterThan) exp).getRightExpression();
                    LongValue value = (LongValue) ((GreaterThan) exp).getLeftExpression();
                    right = (int) value.getValue();

                }
            } else if(exp instanceof GreaterThanEquals) {
                if(((GreaterThanEquals) exp).getLeftExpression() instanceof Column){
                    left =(Column) ((GreaterThanEquals)exp).getLeftExpression();
                    LongValue value =(LongValue)((GreaterThanEquals)exp).getRightExpression();
                    right = (int)value.getValue();
                } else { // left is value
                    left = (Column) ((GreaterThanEquals) exp).getRightExpression();
                    LongValue value = (LongValue) ((GreaterThanEquals) exp).getLeftExpression();
                    right = (int) value.getValue();
                }
            } else if(exp instanceof MinorThan ){
                    if(((MinorThan) exp).getLeftExpression() instanceof Column){
                        left =(Column) ((MinorThan)exp).getLeftExpression();
                        LongValue value =(LongValue)((MinorThan)exp).getRightExpression();
                        right = (int)value.getValue();
                    } else{ // left is value
                        left =(Column) ((MinorThan)exp).getRightExpression();
                        LongValue value =(LongValue)((MinorThan)exp).getLeftExpression();
                        right= (int)value.getValue();
                    }
            } else if(exp instanceof MinorThanEquals) {
                if (((MinorThanEquals) exp).getLeftExpression() instanceof Column) {
                    left = (Column) ((MinorThanEquals) exp).getLeftExpression();
                    LongValue value = (LongValue) ((MinorThanEquals) exp).getRightExpression();
                    right = (int) value.getValue();
                } else { // left is value
                    left = (Column) ((MinorThanEquals) exp).getRightExpression();
                    LongValue value = (LongValue) ((MinorThanEquals) exp).getLeftExpression();
                    right = (int) value.getValue();
                }
            }
            //reduction factor
            String tabName = left.toString().split("\\.")[0];
            String colName = left.toString().split("\\.")[1];
            int[] range = tablestats.get(tabName).getRange(colName);
            double rf =((double)(range[1] - right)) /((double)(range[1] - range[0]));

            if(Vvalues.containsKey(tabName)){
                Vvalues.put(tabName, Math.min(Vvalues.get(tabName),(int)(rf * (range[1]-range[0]+1))));
            } else {
                Vvalues.put(tabName, (int)(rf * (range[1]- range[0] + 1)));
            }
        }

        //process join table
        for(Expression ex : joinTable){
            Column left = (Column)(((EqualsTo)ex).getLeftExpression());
            Column right = (Column)(((EqualsTo)ex).getRightExpression());
            if(left!= null && right != null){
                String leftTabName = left.toString().split("\\.")[0];
                String rightTabName = right.toString().split("\\.")[0];
                if(!Vvalues.containsKey(leftTabName) &&
                        Vvalues.containsKey(rightTabName)){
                    Vvalues.put(leftTabName, Vvalues.get(rightTabName));
                } else if (!Vvalues.containsKey(rightTabName) &&
                        Vvalues.containsKey(leftTabName)){
                    Vvalues.put(rightTabName, Vvalues.get(leftTabName));
                } else if (Vvalues.containsKey(rightTabName) && Vvalues.containsKey(leftTabName)){
                    int min = Math.min(Vvalues.get(leftTabName), Vvalues.get(rightTabName));
                    Vvalues.put(leftTabName, min);
                    Vvalues.put(rightTabName, min);
                } else { // both not exist in the selection condition
                    int[] leftRange =
                            tablestats.get(leftTabName).getRange(left.toString().split("\\.")[1]);
                    int[] rightRange =
                            tablestats.get(rightTabName).getRange(right.toString().split("\\.")[1]);
                    int leftVal = leftRange[1]-leftRange[0]+1;
                    int rightVal = rightRange[1] - rightRange[0]+1;
                    Vvalues.put(leftTabName,Math.min(leftVal,rightVal));
                    Vvalues.put(rightTabName, Math.min(leftVal,rightVal));
                }
            }
        }

    }
}
