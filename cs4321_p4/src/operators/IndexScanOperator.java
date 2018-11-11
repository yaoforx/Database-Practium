package operators;

import btree.BTreeNode;
import btree.Btree;
import btree.BtreeIndexNode;
import btree.BtreeLeafNode;
import net.sf.jsqlparser.statement.select.OrderByElement;
import util.DBCatalog;
import util.Table;
import util.Tuple;
import util.TupleIdentifier;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

public class IndexScanOperator extends ScanOperator {
    private Integer lowKey;
    private Integer highKey;
    private int indexedCol;
    protected File indexfile;
    boolean isClustered;
    private BTreeNode currentLeafNode;
    private TupleIdentifier tpi;
    private Btree btree;
    private int keyPos; // The index of the key that we are currently on within the current leaf node
    private int tupPos; // Index of the tuple we are currently on within the current key within the current leaf

    public IndexScanOperator(Table tb, Integer lowKey, Integer highKey, Btree btree, File indexfile) {
        super(tb);
        this.lowKey = lowKey;
        this.highKey = highKey;
        this.btree = btree;
        keyPos = 0;
        this.indexfile = indexfile;
        if(lowKey == null) {
            //smallest leaf node will always on page 1
            currentLeafNode = btree.getLeafNode(1);
        } else {
            currentLeafNode = btree.deserializeTraversal(lowKey);
            while(currentLeafNode.getKeys(keyPos) < lowKey) keyPos++;
        }
        tupPos = 0;
        tpi = ((BtreeLeafNode)currentLeafNode).getTupleInfo(keyPos, tupPos);
        indexedCol = btree.getIndexedCol();

    }

    @Override
    public Table getTable() {
        return super.getTable();
    }

    @Override
    public Tuple getNextTuple() {
        Tuple tuple = null;

        while(true) {
            if(isClustered) {
                tuple = super.getNextTuple();
                if(tuple == null) {
                    return null;
                }
                if(highKey != null && tuple.tuple.get(indexedCol) > highKey) {
                    return null;
                }
                return tuple;
            } else {
                if(keyPos >= currentLeafNode.numOfKeys()) {
                    if(currentLeafNode.getAddr() + 1 > btree.numOfLeaves) return null;//explored all the leaves
                    currentLeafNode = btree.getLeafNode(currentLeafNode.getAddr() + 1);
                    keyPos = 0;
                    tupPos = 0;
                    continue;
                }
                if(highKey != null && currentLeafNode.getKeys(keyPos) > highKey) return null;
                if(tupPos >= ((BtreeLeafNode)currentLeafNode).sizeOfEntry(keyPos)) {
                    keyPos++;
                    tupPos = 0;
                    continue;
                }
                TupleIdentifier curtpi = ((BtreeLeafNode) currentLeafNode).getTupleInfo(keyPos, tupPos);
                tupPos++;
                try {
                    tuple = lines.read(curtpi);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if(tuple != null) return tuple;
            }
        }
    }

    @Override
    public void reset() {
        super.reset();
    }

    @Override
    public void dump(PrintStream s) {
        super.dump(s);
    }

    @Override
    public String print() {
        String tableName = DBCatalog.alias.containsKey(tb.tableName) ? DBCatalog.alias.get(tb.tableName) : tb.tableName;
        return "IndexScan[" + tableName + "," + indexedCol + "," + lowKey + "," + highKey + "]\n";
    }

    @Override
    public void printTree(PrintStream ps, int lv) {
        printIndent(ps, lv);
        ps.print(print());

    }
}
