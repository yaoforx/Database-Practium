package btree;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeMap;

public class BtreeIndexNode extends BTreeNode {
    List<BTreeNode> children;

    public List<Integer> pointers;
    int minElement;


    public BtreeIndexNode(List<Integer> keys, List<BTreeNode> children, int addr, int order) {
        super(addr, keys, order);
        this.children = children;

    }
    public void setPointers( List<Integer> pointers) {
        this.pointers  = pointers;
    }
    @Override
    public boolean isLeafNode() {
        return false;
    }

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
     * @return
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
     * @return
     */
    @Override
    public int leafNum() {
        int sum = 0;
        for(BTreeNode child : children) {
            sum += child.leafNum();
        }
        return sum;
    }

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
