package util;

import java.util.HashMap;
import java.util.Set;

public class TableStat {
    Table table;
    public HashMap<String, int[]> indexRange;
    int totalTuple;
    int attNum;
    public TableStat(String name, String[] cols, int[] low, int[] high) {
        this.table = DBCatalog.getTable(name);
        for(int i = 0; i < cols.length; i++) {
            int[] range = {low[i], high[i]};
            if(cols[i] != null) indexRange.put(cols[i], range);
        }
        attNum = cols.length;
    }

    public int getAttNum(){
        if(attNum == -1){
            throw new IllegalArgumentException();
        }
        return attNum;
    }
    public int[]  getRange(String attrName){
        if(indexRange.containsKey(attrName)){
            return indexRange.get(attrName);
        } else {
            return null;
        }
    }


    /**
     * return a list of attr names corresponding to this table
     * @return
     */
    public String[] getAttrName(){
        Set<String> attrName = indexRange.keySet();
        String[] list = (String[])(attrName.toArray());
        return list;
    }
    /**
     * return the table name
     * @return
     */
    public String getTableName(){
        return table.tableName;
    }
    /**
     * add new attr to the current table
     *
     */
    public void addCol(String attrName, int low, int high){
        if(indexRange.containsKey(attrName)){
            throw new IllegalArgumentException();
        } else {
            int[] range = {low, high};
            indexRange.put(attrName,range);
        }
    }

    /**
     * set the tuple number for this table
     */
    public void setTpNum(int num){
        totalTuple = num;
    }
    /**
     *
     * @return num of tps in this table
     */
    public int getTotoalTps(){
        return totalTuple;
    }



}
