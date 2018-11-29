package optimal;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import unionfind.UnionFind;
import unionfind.UnionFindElement;
import util.DBCatalog;
import util.Selector;
import util.TableStat;
import util.Util;

import javax.xml.catalog.Catalog;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Vvalues class constructs V-value for every contribute
 * @author Yao Xiao
 */
public class Vvalues {
    //key is table name, value is V value for each table and table's cols
    private Map<String, Double> Vvalue;
    private TableStat V; // this map stores the range value to the respective column.

    private String table;
    public Vvalues(String table){
        Vvalue = new HashMap<>();
        V = DBCatalog.tablestats.get(Util.getFullTableName(table));
        this.table = table;
        Vvalue.put(table, V.getTotoalTps() * V.getAttNum() * 4/4096*1.0);
        buildValue();

    }

    /**
     * this method build the V value to their respective column name.
     * Store the result in the V value hash map.
     */
    private void buildValue(){

        List<String> columns = DBCatalog.getSchema(table);
        int[][] initial = new int[columns.size()][2];
        double value = 1.0;
        for(int i = 0; i < columns.size(); i++) {
            initial[i] = V.getIndexRange(V.getColName(i));
        }

        for(int i=0;i<columns.size();i++) {
            value = initial[i][1] - initial[i][0];
            String temp = table + "." + columns.get(i);
            int[] range = V.getIndexRange(temp);
            double difference = range[1] - range[0] + 1.0;
            Integer[] updateRange = OptimalSelect.columnInfo.get(temp);
            if (updateRange == null) {
               updateRange = new Integer[2];
               updateRange[0] = range[0];
                updateRange[1] = range[1];



            } else {
                if (updateRange[0] == null) {
                    updateRange[0] = range[0];
                }
                if (updateRange[1] == null) {
                    updateRange[1] = range[1];
                }
            }



            double V = (double) updateRange[1] - updateRange[0] - 1;
            Vvalue.put(temp, V);
            value = value * V /difference;

        }
        if(value==0.0) value = 1.0;
        Vvalue.put(table, value);
        for(Map.Entry<String, Double> entry : Vvalue.entrySet())
            Vvalue.put(entry.getKey(), Math.min(entry.getValue(),value));
    }

    /**
     * Return the map associated with tale name
     * @return map contains each v-value for this table
     */
    public Map<String,Double> getV() {
        return Vvalue;
    }

    public boolean contains(String tbName){
        if(Vvalue.containsKey(tbName)) return true;
        return false;
    }




}
