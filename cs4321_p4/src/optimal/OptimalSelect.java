package optimal;

import btree.Btree;
import btree.BtreeIndexNode;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import util.DBCatalog;
import util.TableStat;
import util.Util;
import util.indexInfo;

import java.util.*;

public class OptimalSelect {
    static List<String> columnName;
    static List<Double> scanCost;
    private static double plainScan;
    public static Vsets vvalues = new Vsets();

    public static HashMap<String, Integer[]> columnInfo;

    public OptimalSelect(){
        columnName = new ArrayList<>();
        scanCost = new ArrayList<>();
        columnInfo = new HashMap<>();


    }
    public static indexInfo calculateAndChoose(String tableName, Expression expression) {
        plainScan = -1;
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

                plainScan =1.0* (DBCatalog.tablestats.get(tableName).getTotoalTps() *
                        DBCatalog.tablestats.get(tableName).getAttNum() * 4)/4096;
                continue;
            }else if (exp instanceof EqualsTo){
                plainScan = 1.0*(DBCatalog.tablestats.get(tableName).getTotoalTps() *
                        DBCatalog.tablestats.get(tableName).getAttNum() * 4)/4096;
                continue;
            }
            String[] s = {col};
            Integer[] range = Util.getSelRange(exp,s);
            updateInfo(tableName + "." + col,range);

        }



        tableName = Util.getFullTableName(tableName);
        long tupleNum = DBCatalog.tablestats.get(tableName).getAttNum() *
                DBCatalog.tablestats.get(tableName).getTotoalTps() * 4;
        int pageNum = (int)tupleNum/4096 + 1;
        Set<Map.Entry<String, Integer[]>> entries = columnInfo.entrySet();
        for(Map.Entry<String, Integer[]> entry : entries) {
            HashMap<String, indexInfo> tableInfo = DBCatalog.indexes.get(tableName);
            double localCost = 0;
            if(tableInfo == null) {
                localCost = pageNum;
                plainScan = pageNum;
            }
            else {
                //tableName = Util.getFullTableName(tableName);

                indexInfo info = tableInfo.get(tableName);

                int[] range = DBCatalog.tablestats.get(tableName).getIndexRange(entry.getKey());
                if(range == null) continue;

                double maxRange = range[1] - range[0];
                double curHigh = range[1];
                double curLow = range[0];
                if(entry.getValue()[1] != null) {
                    curHigh = entry.getValue()[1];
                }
                if(entry.getValue()[0] != null) {
                    curLow = entry.getValue()[0];
                }
                double reductionFactor = (curHigh - curLow)/maxRange;
                if(reductionFactor <= 0) reductionFactor = 1.0;
                if(info != null && info.clustered) {
                    // choose 1 as tree height
                    localCost = 1 + pageNum * reductionFactor;
                } else {
                    Btree btree =  DBCatalog.idxConfig.loaders.get(tableName).getBtree();
                    localCost = 1 + (btree.numOfLeaves +  DBCatalog.tablestats.get(tableName).getTotoalTps()) * reductionFactor;

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
        if(!vvalues.contains(tableName)) {
            vvalues.sets.add(new Vvalues(tableName));
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
