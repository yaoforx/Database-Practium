package btree;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * an abstract class holds info can be used by b+ tree index node and lead node
 * @author Yao Xiao
 */
public abstract class BTreeNode {
    protected List<Integer> keys;
    protected int addr;
    int order = 0;
    public BTreeNode(int address, List<Integer> keys, int order) {
        this.addr = address;
        this.keys = keys;
        this.order = order;
    }
    public int getKeys(int index) {
        return keys.get(index);
    }
    public int getAddr() {
        return addr;
    }
    public int numOfKeys() {
        return keys.size();
    }
    public abstract boolean isLeafNode();
    public abstract List<BTreeNode> getChildren();
    /**
     * Returns the smallest search key in the leftmost leaf node
     * of this subtree.
     */
    public abstract int getSmallest();

    /**
     * get number of nodes in this subtree
     * @return
     */
    public abstract int getSize();

    /**
     * get number of leaf nodes in this subtree
     * @return
     */
    public abstract int leafNum();
    public abstract void serialize(ByteBuffer buffer);

}
