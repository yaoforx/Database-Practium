package util;

/**
 * Class for every table to record its index information
 * @author  Yao Xiao
 */
public class indexInfo {
    public String tab;
    public String indexCol;
    public boolean clustered = false;
    public int order = 1;
    public indexInfo(String t, String index, boolean cluster, int ord) {
        this.tab = t;
        this.indexCol = index;
        this.clustered = cluster;
        this.order = ord;
    }
}
