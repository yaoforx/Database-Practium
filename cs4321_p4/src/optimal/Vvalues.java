package optimal;

import util.DBCatalog;
import util.Table;
import util.TableStat;

import java.beans.Expression;
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

    }
}
