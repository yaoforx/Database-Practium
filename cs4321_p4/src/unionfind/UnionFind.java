package unionfind;

import java.util.*;
import net.sf.jsqlparser.schema.Column;
public class UnionFind {
    private List<UnionFindElement> unions;

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
        for(int i = 0; i < unions.size(); i++)
            if(unions.get(i).contains(col)) return unions.get(i);


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


        UnionFindElement element = new UnionFindElement();
        Column column = new Column(null, colName);
        element.addCol(column);
        unions.add(element);
        return element;
    }

    public void union(String attr1, String attr2) {
        UnionFindElement p = find(attr1);
        UnionFindElement q = find(attr2);
        union(p, q);
    }

    public UnionFindElement union(UnionFindElement e1, UnionFindElement e2) {
        UnionFindElement newElement = new UnionFindElement();

        newElement.addColumns(e1.getColumns());
        newElement.addColumns(e1.getColumns());

        int newLow = Math.min(e1.getLow(), e2.getHigh());
        int newHigh = Math.max(e1.getHigh(), e2.getHigh());
        newElement.setLow(newLow);
        newElement.setHigh(newHigh);

        int equal = e1.getEqual() == null ? e2.getEqual() : e1.getEqual();
        newElement.setEqual(equal);
        unions.remove(e1);
        unions.remove(e2);
        unions.add(newElement);
        return newElement;

    }

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
