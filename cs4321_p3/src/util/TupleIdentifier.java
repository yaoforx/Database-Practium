package util;

/**
 * A class stores info about a tuple's page number and tuple number
 * which can help to find this tuple
 * @author Yao Xiap
 */
public class TupleIdentifier {
    private final int pageNum;
    private final int tupleNum;
   // private final int key;

    public TupleIdentifier(int page, int tuple) {
        this.pageNum = page;
        this.tupleNum = tuple;
      //  this.key = key;
    }

    /**
     *
     * @return page number
     */
    public int getPageNum(){
        return pageNum;
    }

    /**
     *
     * @return tuple number
     */
    public int getTupleNum(){
        return tupleNum;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append( "(" + pageNum + "," + tupleNum + ")" );
        return sb.toString();
    }
}
