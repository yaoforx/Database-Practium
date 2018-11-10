package unionfind;

import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.List;

public class UnionFindElement {
    private List<Column> columns;
    private int low;
    private int high;
    private Integer equalNum = null;

    public UnionFindElement() {
        columns = new ArrayList<>();
        low = Integer.MIN_VALUE;
        high = Integer.MAX_VALUE;

    }
    public List<Column> getColumns(){return columns;}
    public int getLow(){
        return low;
    }

    /**
     * Get the upper bound of the unionFind
     * @return int, upper bound
     */
    public int getHigh(){
        return high;
    }

    public Integer getEqual() {return equalNum;}

    /**
     * Set the lower bound
     * @param bound, int
     */
    public void setLow(int bound){
        low = bound;
    }

    /**
     * Set the upper bound
     * @param bound,int
     */
    public void setHigh(int bound){
        high = bound;
    }

    /**
     * Set the equality constraints
     * @param bound,int
     */
    public void setEqual(int bound){
        equalNum = bound;
        low = bound;
        high = bound;
    }


    /**
     * Add an attribute to the UnionFind
     * @param
     */
    public void addCol(Column attr){
        if (!columns.contains(attr)){
            columns.add(attr);
        }
    }

    /**
     * Add a list of attributes to the UnionFind
     * @param
     */
    public void addColumns(List<Column> Attrs){
        for (int i = 0; i < Attrs.size(); i++){
            if (!columns.contains(Attrs.get(i))){
                columns.add(Attrs.get(i));
            }
        }
    }

    public boolean contains(Column col){
        for (Column column : columns){
            if ((col.getWholeColumnName()).equals(column.getWholeColumnName())){
                return true;
            }
        }
        return false;
    }
    @Override
    public String toString(){
        if (columns.size() > 0){
            StringBuilder sb = new StringBuilder();
            sb.append("[[");
            sb.append(columns.get(0));
            for (int i = 1; i < columns.size(); i++){
                sb.append(", ");
                sb.append(columns.get(i).toString());
            }
            if (equalNum == (Integer) null){
                sb.append("], equals " + "null");
            }else{
                sb.append("], equals " + equalNum.toString());
            }

            if (low == Integer.MIN_VALUE){
                sb.append(", min " + null);
            }else{
                sb.append(", min " + low);
            }

            if (high == Integer.MAX_VALUE){
                sb.append(", max " + null);
            }else{
                sb.append(", max " + high);
            }
            sb.append("]");
            return sb.toString();
        }
        return null;
    }



}
