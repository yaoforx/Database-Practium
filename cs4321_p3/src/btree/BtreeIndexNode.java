package btree;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeMap;

/**
 * Class for serializing and deserialize a B+tree Index Node
 * @author Yao Xiao
 */
public class BtreeIndexNode extends BTreeNode {
    List<BTreeNode> children;

    public List<Integer> pointers;

    public BtreeIndexNode(List<Integer> keys, List<BTreeNode> children, int addr, int order) {
        super(addr, keys, order);
        this.children = children;

    }

    /**
     * A method to set up pointers, not in use
     * @param pointers
     */
    public void setPointers( List<Integer> pointers) {
        this.pointers  = pointers;
    }

    /**
     * True if this is a leaf Node
     * @return always false;
     */
    @Override
    public boolean isLeafNode() {
        return false;
    }

    /**
     * get all children of this index node
     * @return a list of BtreeNode
     */
    @Override
    public List<BTreeNode> getChildren() {
        return children;
    }

    @Override
    public int getSmallest() {
        return children.get(0).getSmallest();
    }

    /**
     * get the total size of the subtree including its children
     * @return size
     */
    @Override
    public int getSize() {
        int sum = 1;
        for(BTreeNode child : children) {
            sum += child.getSize();
        }
        return sum ;
    }

    /**
     * get the total size of the subtree leaves
     * @return number of leaves under this node
     */
    @Override
    public int leafNum() {
        int sum = 0;
        for(BTreeNode child : children) {
            sum += child.leafNum();
        }
        return sum;
    }

    /**
     * serialize the Index node with buffer
     * @param buffer
     */
    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putInt(0, 1);
        //1 represents index node
        // number of keys in this node
        buffer.putInt(4, numOfKeys());
        int index = 8;
        for(Integer key : keys) {
            buffer.putInt(index, key);
            index += 4;
        }
        for(BTreeNode child : children) {
            buffer.putInt(index, child.addr);
            index += 4;
        }

    }


    /**
     * Method for matching with expected output format
     * @return string format of Index node
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IndexNode with keys [");
        for (Integer key : keys) {
            sb.append(key + ", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append("] and child addresses [");
        for (BTreeNode child : this.children) {
            sb.append(Integer.valueOf(child.addr) + ", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append("]\n");
        return sb.toString();
    }

}
