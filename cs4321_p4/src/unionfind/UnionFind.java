package unionfind;

import java.util.*;

import javafx.scene.control.Tab;
import net.sf.jsqlparser.schema.Column;
import util.*;

public class UnionFind {
    private static List<UnionFindElement> unions;

    public UnionFind(){
        unions = new ArrayList<>();
    }

    public Set<UnionFindElement> findElements(String tableName) {
        Set<UnionFindElement> res = new HashSet<>();
        for(UnionFindElement element : unions) {
            for(Column column : element.getColumns()) {
                if(column.getWholeColumnName().startsWith(tableName)) {
                    res.add(element);
                }
            }
        }
        return res;
    }
    public List<UnionFindElement> getUnions(){
        return unions;
    }

    public UnionFindElement find(Column col) {
        for(int i = 0; i < unions.size(); i++) {
            if (unions.get(i).contains(col)) return unions.get(i);
        }


        UnionFindElement element = new UnionFindElement();
        element.addCol(col);
        unions.add(element);
        return element;
    }

    public UnionFindElement find(String colName) {
        for(int i = 0; i < unions.size(); i++)
           for(int j = 0; j < unions.get(i).getColumns().size(); j++) {
               if(colName.equals(unions.get(i).getCol(j))) return unions.get(i);
           }

         return createElement(colName);

    }
    private UnionFindElement createElement(String name) {
        UnionFindElement ele = new UnionFindElement();
        String[] names = name.split("\\.");
        name = Util.getFullTableName(names[0]);
        TableStat st = DBCatalog.tablestats.get(name);
        int[] range = st.getIndexRange(names[1]);
        ele.setLow(range[0]);
        ele.setHigh(range[1]);
        Column column = new Column(null, names[1]);
        ele.addCol(column);
        unions.add(ele);
        return ele;


    }

    public void union(String attr1, String attr2) {
        UnionFindElement p = find(attr1);
        UnionFindElement q = find(attr2);
        union(p, q);
    }

    public UnionFindElement union(UnionFindElement e1, UnionFindElement e2) {
        UnionFindElement newElement = new UnionFindElement();

        newElement.addColumns(e1.getColumns());
        newElement.addColumns(e2.getColumns());

        int newLow = Math.min(e1.getLow(), e2.getHigh());
        int newHigh = Math.max(e1.getHigh(), e2.getHigh());
        newElement.setLow(newLow);
        newElement.setHigh(newHigh);

        Integer equal = e1.getEqual() == null ? e2.getEqual() : e1.getEqual();
        if(equal != null) {
            newElement.setEqual(equal);
        }
        unions.remove(e1);
        unions.remove(e2);
        unions.add(newElement);
        return newElement;

    }
    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < unions.size(); i++){
            if (i > 0){
                sb.append("\n");
            }
            sb.append(unions.get(i).toString());
        }
        return sb.toString();
    }

}
