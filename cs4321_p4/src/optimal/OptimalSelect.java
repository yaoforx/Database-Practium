package optimal;

import btree.Btree;
import btree.BtreeIndexNode;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import util.DBCatalog;
import util.Util;
import util.indexInfo;

import java.util.*;

public class OptimalSelect {
    static List<String> columnName;
    static List<Double> scanCost;
    static HashMap<String, Integer[]> columnInfo;
    public static indexInfo calculateAndChoose(String tableName, Expression expression) {
        columnInfo = new HashMap<>();
        columnName = new ArrayList<>();
        scanCost = new ArrayList<>();

        double curCost = 1.0;
        List<Expression> exps = Util.getAndExpressions(expression);

        for(Expression exp : exps) {
            Expression left = ((BinaryExpression) exp).getLeftExpression();
            Expression right = ((BinaryExpression) exp).getRightExpression();

            String col = null;

            if(left instanceof Column && right instanceof LongValue) {
                //String[] split =((Column)left).toString().split("\\.");
                col = ((Column)left).toString().split("\\.")[1];
            } else if(right instanceof Column && left instanceof LongValue){
                col = ((Column)right).toString().split("\\.")[1];
            } else if(right instanceof Column && left instanceof Column){
                //calculate plain cost

                curCost =1.0* (DBCatalog.tablestats.get(tableName).getTotoalTps() *
                        DBCatalog.tablestats.get(tableName).getAttNum() * 4)/4096;
                continue;
            }else if (exp instanceof EqualsTo){
               curCost = 1.0*(DBCatalog.tablestats.get(tableName).getTotoalTps() *
                        DBCatalog.tablestats.get(tableName).getAttNum() * 4)/4096;
                continue;
            }
            String[] s = {col};
            Integer[] range = Util.getSelRange(exp,s);
            updateInfo(col,range);


        }
        long tupleNum = DBCatalog.tablestats.get(tableName).getAttNum() *
                DBCatalog.tablestats.get(tableName).getTotoalTps() * 4;
        int pageNum = (int)tupleNum/4096;
        Set<Map.Entry<String, Integer[]>> entries = columnInfo.entrySet();
        for(Map.Entry<String, Integer[]> entry : entries) {
            indexInfo info = DBCatalog.indexes.get(tableName).get(entry.getKey());
            double localCost = 0;
            if(info == null) localCost = pageNum;
            else {
                int[] range = DBCatalog.tablestats.get(tableName).getRange(entry.getKey());
                double maxRange = range[1] - range[0];
                double curHigh = Double.MIN_VALUE;
                double curLow = Double.MAX_VALUE;
                if(entry.getValue()[0] == null) curHigh = range[1];
                if(entry.getValue()[1] == null) curLow = range[0];
                double reductionFactor = (curHigh - curLow)/maxRange;

                if(info.clustered) {
                    // choose 3 as tree height
                    localCost = 3 + pageNum * reductionFactor;
                } else {
                    Btree btree =  DBCatalog.idxConfig.loaders.get(info.tab).getBtree();
                    localCost = 3 + (btree.numOfLeaves +  DBCatalog.tablestats.get(tableName).getTotoalTps()) * reductionFactor;

                }
            }
            columnName.add(entry.getKey());
            scanCost.add(localCost);
        }
        if(scanCost.isEmpty()) return null;
        int minIndex = 0;
        double minCost = scanCost.get(0);
        for(int i = 1; i < scanCost.size(); i++){
            if(minCost > scanCost.get(i)){
                minCost = scanCost.get(i);
                minIndex = i;
            }
        }
        String colName = DBCatalog.schemas.get(tableName).get(minIndex);
        indexInfo winner = DBCatalog.getindexInfo(tableName, colName);

        return winner;


    }
    public static void updateInfo(String attr, Integer[] range){
        if(!columnInfo.containsKey(attr)){
            Integer[] r = new Integer[2];
            for(int i = 0; i < range.length; i++){
                r[i] = range[i];
            }
            columnInfo.put(attr, r);
        } else {
            Integer[] preRange = columnInfo.get(attr);
            // update min
            if(range[0]!=null){
                if(preRange[0] == null){
                    preRange[0] = range[0];
                } else {
                    preRange[0] = Math.max(preRange[0],range[0]);
                    columnInfo.put(attr, preRange);
                }
            }
            //update max
            if(range[1]!=null){
                if(preRange[1] == null){
                    preRange[1] = range[1];
                } else {
                    preRange[1] = Math.min(preRange[1],range[1]);
                    columnInfo.put(attr, preRange);
                }
            }
        }
    }

}
