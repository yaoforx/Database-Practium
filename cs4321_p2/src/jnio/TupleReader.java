//package jnio;
//
//import util.Tuple;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.nio.channels.FileChannel;
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * TupleReader reads tuple page by page
// */
//
//public class TupleReader {
//
//    private ByteBuffer page;
//    private static final int size = 4096;
//    private boolean eof = false;
//    private int colIntuple;
//    private int tupleInPgae;
//    private File file;
//    private FileChannel fc;
//    private boolean newPage;
//    private int currentSize;
//    private int currentPage;
//    private int currentTuple;
//    private int position;
//
//
//
//
//    /**
//     * Tuple Reader Constructor
//     * @param file to read from
//     * @throws IOException
//     */
//    public TupleReader(File file) throws IOException {
//        this.file = file;
//        FileInputStream fin = new FileInputStream(this.file);
//        fc = fin.getChannel();
//        page = ByteBuffer.allocate(4096);
//        eof = false;
//        newPage = true;
//        colIntuple = 0;
//        tupleInPgae = 0;
//        currentPage = -1;
//        currentTuple = 0;
//    }
//
//    /**
//     * Function: Read tuples from bufferpage with bounded state
//     * @return tuple
//     * @throws IOException
//     */
//    public Tuple read() throws IOException {
//        while (!eof) {
//            if (newPage) {
//                eof = (fc.read(page) <= 0);
//                if (eof) return null;
//                currentPage++;
//                currentTuple = 0;
//                setPage();
//            }
//            if (page.hasRemaining()) {
//
//                List<Integer> tuple = new ArrayList<>();
//                for (int i = 0; i < colIntuple; i++) {
//                    tuple.add(Integer.valueOf(page.getInt()));
//
//                }
//
//                Tuple tp = new Tuple(tuple);
//                tp.setPageNum(currentPage);
//                tp.setIdxInPage(currentTuple);
//                currentTuple++;
//                return tp;
//            }
//            newPage = true;
//            setZeros();
//        }
//        return null;
//    }
//
//    /**
//     * Function: setPage
//     * Set a new page whenever a previous page is exhausted
//     */
//
//    private void setPage() throws IOException{
//
//        page.flip();
//        colIntuple = page.getInt();
//        tupleInPgae = page.getInt();
//        currentSize = (colIntuple * tupleInPgae * 4 + 8);
//        page.limit(currentSize);
//
//        newPage = false;
//    }
//
//
//    /**
//     * Function: serZeros
//     * Set remaining page to zeros
//     */
//    private void setZeros()
//    {
//        page.clear();
//        page.put(new byte[4096 - currentSize]);
//        page.clear();
//    }
//
//
//    /**
//     * Reset method that resets everything
//     * Let Tuple reader start from very begining
//     */
//    public void reset() throws IOException {
//       close();
//        FileInputStream fin = new FileInputStream(this.file);
//        fc = fin.getChannel();
//        page = ByteBuffer.allocate(4096);
//        eof = false;
//        newPage = true;
//        colIntuple = 0;
//        tupleInPgae = 0;
//        currentPage = 0;
//        currentTuple = 0;
//
//    }
//    /**
//     * Reset method that reset reader to the tuple position specified
//     */
//    public void reset(int pageNum, int pos) throws IOException{
//        if(pos >= tupleInPgae) {
//             throw new IndexOutOfBoundsException("The index is too large: " + pos);
//        }
//        fc.position((long)pageNum * size);
//        this.page.clear();
//        this.page.put(new byte[size]);
//        this.page.clear();
//        newPage = true;
//        eof = false;
//        currentTuple = pos;
//
//
//        eof = (fc.read(page) < 0);
//        if (eof) return;
//        page.flip();
//        colIntuple = page.getInt();
//        tupleInPgae = page.getInt();
//        currentSize = (colIntuple * tupleInPgae * 4 + 8);
//        page.limit(currentSize);
//
//        newPage = false;
//        page.position(pos * colIntuple * 4 + 8);
//
//    }
//
//    /**
//     * Function: close
//     * close reader files and associated bufferchannels
//     * @throws IOException
//     */
//    public void close() throws IOException
//    {
//        page.clear();
//        fc.close();
//    }
//
//    public void reset(int index){
//        try{
//            fc = fc.position(0);
//            page.clear();
//            if (fc.read(page) != -1){
//                tupleInPgae = page.getInt(4);
//                page.clear();
//                int fcPosition = index/tupleInPgae *size ;
//                fc = fc.position(fcPosition);
//                if (fc.read(page) != -1){
//                    currentPage = index/tupleInPgae;
//                    currentTuple = index%tupleInPgae;
//                    colIntuple = page.getInt(0);
//                    position = (index%tupleInPgae) * colIntuple * 4 + 8;
//                    tupleInPgae = page.getInt(4) - index%(tupleInPgae);
//                }else{
//                    throw new IndexOutOfBoundsException();
//                }
//            }else{
//                throw new FileNotFoundException();
//            }
//        } catch (IOException e) {
//            System.err.println("Error occured while reading buffer");
//            e.printStackTrace();
//        }
//    }
//}

package jnio;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import util.*;


/**
 * The <tt>BinaryTupleReader</tt> class is a convenience class for reading
 * tuples from a file of binary format representing the tuples.
 * <p>
 * The tuple reader uses Java's <em>NIO</em> which reads file in blocks to speed
 * up the operation of I/O.
 * <p>
 * The format of the input file is: data file is a sequence of pages of 4096
 * bytes in size. Every page contains two pieces of metadata. tuple themselves
 * are stored as (four-byte) integers.
 *
 * @author Chengxiang Ren (cr486)
 *
 */
public final class TupleReader  {
    private File file;						// the file reading
    private static final int B_SIZE = 4096;	// the size of the buffer page
    private static final int INT_LEN = 4;	// the bytes a integer occupies
    private FileChannel fc;					// The file channel for reader
    private ByteBuffer buffer;				// The buffer page.
    private int numOfAttr;					// Number of attributes in a tuple
    private int numOfTuples;				// Number of tuples in the page
    private long currTupleIdx;				// the index of current tuple
    private boolean needNewPage;			// flag for read new page to buffer
    private boolean endOfFile;				// flag for end of file
    private List<Long> offsets;				// the list stores the maximum
    // tuple index in the page at
    // current index.
    /**
     * Creates a new <tt>BinaryTupleReader</tt>, given the <tt>File</tt> to
     * read from.
     *
     * @param file the <tt>File</tt> to read from
     * @throws FileNotFoundException if the file does not exist or cannot be
     * 		   opened for reading.
     */
    public TupleReader(File file) throws FileNotFoundException {
        this.file = file;
        fc = new FileInputStream(file).getChannel();
        buffer = ByteBuffer.allocate(B_SIZE);
        endOfFile = false;
        needNewPage = true;
        offsets = new ArrayList<Long>();
        offsets.add(new Long(0));
        currTupleIdx = 0;
    }

    /**
     * Creates a new <tt>BinaryTupleReader</tt>, given the name of file to
     * read from.
     *
     * @param fileName the name of the file to read from
     * @throws FileNotFoundException if the file does not exist or cannot be
     * 		   opened for reading.
     */
    public TupleReader(String fileName) throws FileNotFoundException {
        this(new File(fileName));
    }

    /**
     * read the next tuple from the table.
     *
     * @return Tuple the tuple at the reader's current position
     * 		   <tt>null</tt> if reached the end of the file
     * @throws IOException If an I/O error occurs while calling the underlying
     * 					 	reader's read method
     *
     */

    public Tuple read() throws IOException {
        while (!endOfFile) {
            // read a new page into the buffer and set the metadata accordingly
            if (needNewPage) {
                try {
                    fetchPage();
                } catch (EOFException e) {
                    break;
                }
                //System.out.println("============ page " + offsets.size()
                //	+ "=== tuple " + numOfTuples +" =======");
            }

            if (buffer.hasRemaining()) {
                List<Integer> tuple = new ArrayList<>();
                for (int i = 0; i < numOfAttr; i++) {
                    tuple.add(Integer.valueOf(buffer.getInt()));

                }

                currTupleIdx++;
                return new Tuple(tuple);
            }

            // does not has remaining
            eraseBuffer();
            needNewPage = true;
        }

        return null;	// if reached the end of the file, return null
    }

    /**
     * Resets the reader to the specified tuple index. the index should be
     * smaller than the tuple index the reader currently at.
     *
     * @param index the tuple index.
     * @throws IOException If an I/O error occurs while calling the underlying
     * 					 	reader's read method
     * @throws IndexOutOfBoundsException unless <tt>0 &le; index &lt; currIndex</tt>
     */

    public void reset(long index) throws IOException, IndexOutOfBoundsException {
        // precondition: the index should not exceed the number of tuples buffered
        if (index >= currTupleIdx || index < 0) {
            throw new IndexOutOfBoundsException("The index is too large");
        }
        int pageIdx = Collections.binarySearch(offsets, new Long(index + 1));
        pageIdx = pageIdx >= 0 ? pageIdx : -(pageIdx + 1);
        //System.out.println("the pageIdx is: " + pageIdx);
        fc.position((long) (pageIdx - 1) * B_SIZE);

        // reset the page containing the tuple
        eraseBuffer();
        needNewPage = true;
        endOfFile = false;
        offsets = offsets.subList(0, pageIdx);
        currTupleIdx = index;
        long numTuplesBuffered = offsets.get(offsets.size() - 1);

        // go to the exact position
        int newTupleOffset = (int) (index - numTuplesBuffered);
        int newPos = (newTupleOffset * numOfAttr + 2) * INT_LEN;

        // fetch the page
        try {
            fetchPage();
        } catch (EOFException e) {
            e.printStackTrace();
        }

        buffer.position(newPos);

    }


    /**
     * Resets the reader to the beginning of the file.
     *
     * @throws IOException If an I/O error occurs while calling the underlying
     * 					 	reader's read method
     */

    public void reset() throws IOException {
        close();
        fc = new FileInputStream(file).getChannel();
        buffer = ByteBuffer.allocate(B_SIZE);
        endOfFile = false;
        needNewPage = true;
        offsets = new ArrayList<Long>();
        offsets.add(new Long(0));
        currTupleIdx = 0;
    }

    /**
     * closes the target
     *
     * @throws IOException If an I/O error occurs while calling the underlying
     * 					 	reader's close method
     */

    public void close() throws IOException {
        fc.close();
    }

    // Helper method for reading a new page into the buffer
    private void fetchPage() throws IOException {
        endOfFile = (fc.read(buffer) < 0);
        needNewPage = false;

        if (endOfFile) throw new EOFException();

        buffer.flip();
        numOfAttr = buffer.getInt();	// metadata 0
        numOfTuples = buffer.getInt();	// metadata 1
        offsets.add(offsets.get(offsets.size() - 1) + numOfTuples);
        // set the limit according to the number of tuples and
        // attributes actually in the page.
        int newLim = (numOfAttr * numOfTuples + 2) * INT_LEN;
        if (newLim >= B_SIZE+ 1) {
            //System.out.println(this.file.getAbsolutePath() + " the new limit is" + newLim);
        }
        buffer.limit((numOfAttr * numOfTuples + 2) * INT_LEN);
    }

    // Helper method that erases the buffer by filling zeros.
    private void eraseBuffer() {
        buffer.clear();
        // fill with 0 to clear the buffer
        buffer.put(new byte[B_SIZE]);
        buffer.clear();
    }

    // Helper method that dumps the file to the System.out for ease of debugging
    private void dump() throws IOException {
        while (fc.read(buffer) > 0) {
            buffer.flip();

            int attr = buffer.getInt();
            int tuples = buffer.getInt();
            //System.out.println("============ page " + count
            // + "=== tuple " + tuples +" =======");
            int col = 0;
            buffer.limit((attr * tuples + 2) * INT_LEN);
            while (buffer.hasRemaining()) {
                col++;
                //System.out.print(buffer.getInt());
                if (col == attr) {
                    System.out.println();
                    col = 0;
                } else {
                    System.out.print(",");
                }
            }

            buffer.clear();
            // fill with 0 to clear the buffer
            buffer.put(new byte[B_SIZE]);
            buffer.clear();
            System.out.println();

        }

    }
}
