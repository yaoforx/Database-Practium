package jnio;

import util.Tuple;
import util.TupleIdentifier;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * TupleReader reads tuple page by page
 * @author Yao Xiao
 */

public class TupleReader {
    /***
     * private ByteBuffer page;
     * int size: page size
     * boolean eof : End Of File
     * int colIntuple: meta data, tuple's size
     * int tupleInPgae: meta data, number of tuples in a page
     * File file: file to read
     * FileChannel fc;
     * boolean newPage: indicates whether we need a new page
     * int currentSize: size containing actual tuples
     * List<Integer> tupleList: num of tuples per page
     */
    private ByteBuffer page;
    private static final int size = 4096;
    private boolean eof = false;
    private int colIntuple;
    private int tupleInPgae;
    private File file;
    private FileChannel fc;
    private boolean newPage;
    private int currentSize;
    private List<Integer> tupleList;






    /**
     * Tuple Reader Constructor
     * @param file to read from
     * @throws IOException
     */
    public TupleReader(File file) throws IOException {
        this.file = file;
        FileInputStream fin = new FileInputStream(this.file);
        fc = fin.getChannel();
        page = ByteBuffer.allocate(4096);
        eof = false;
        newPage = true;
        colIntuple = 0;
        tupleInPgae = 0;
        tupleList = new ArrayList<>();
        tupleList.add(0);



    }

    /**
     * Function: Read tuples from bufferpage with bounded state
     * @return tuple
     * @throws IOException
     */
    public Tuple read() throws IOException {
        while (!eof) {
            if (newPage) {
                eof = (fc.read(page) <= 0);
                if (eof) return null;
                setPage();
            }
            if (page.hasRemaining()) {
                List<Integer> tuple = new ArrayList<>();
                for (int i = 0; i < colIntuple; i++) {
                    tuple.add(Integer.valueOf(page.getInt()));
                }
                Tuple tp = new Tuple(tuple);
                return tp;
            }
            newPage = true;
            setZeros();
        }
        return null;
    }

    /**
     * Read a page
     */
    /**
     * Function: Read tuples from buffer page with bounded state
     * @return tuple
     * @throws IOException
     */
    public ArrayList<Tuple> readNextPage() throws IOException {
        ArrayList<Tuple> tps = new ArrayList<Tuple>();
        while (!eof) {
            if (newPage) {
                eof = (fc.read(page) <= 0);
                if (eof) return null;
                setPage();
            }
            while (page.hasRemaining()) {
                List<Integer> tuple = new ArrayList<>();
                for (int i = 0; i < colIntuple; i++) {
                    tuple.add(Integer.valueOf(page.getInt()));
                }
                Tuple tp = new Tuple(tuple);
                tps.add(tp);
            }

            newPage = true;
            setZeros();
            if(!tps.isEmpty()) return tps;
        }
        return null;
    }


    /**
     * Function: setPage
     * Set a new page whenever a previous page is exhausted
     */

    private void setPage() throws IOException{

        page.flip();
        colIntuple = page.getInt();
        tupleInPgae = page.getInt();
        currentSize = (colIntuple * tupleInPgae * 4 + 8);
        page.limit(currentSize);

        tupleList.add(tupleList.get(tupleList.size() - 1) + tupleInPgae);


        newPage = false;
    }


    public Tuple read(TupleIdentifier tpi) throws IOException {
        // precondition: rid is not null
        if (tpi == null) return null;
        int pageId = tpi.getPageNum();
        int tupleId = tpi.getTupleNum();
        // precondition: the index should not exceed the number of tuples buffered
        if (pageId < 0 || tupleId < 0) {
            throw new IndexOutOfBoundsException("Index out of bound");
        }
        setZeros();
        newPage = true;
        eof = false;
        // load the page

        long position = size * (long) pageId;
        fc.position(position);
        setPage();


        int newPos = (tupleId * colIntuple + 2) * 4;
        page.position(newPos);
        return read();
    }


    /**
     * Function: serZeros
     * Set remaining page to zeros
     */
    private void setZeros()
    {
        page.clear();
        page.put(new byte[4096 - currentSize]);
        page.clear();
    }


    /**
     * Reset method that resets everything
     * Let Tuple reader start from very begining
     */
    public void reset() throws IOException {
       close();
        FileInputStream fin = new FileInputStream(this.file);
        fc = fin.getChannel();
        page = ByteBuffer.allocate(4096);
        eof = false;
        newPage = true;
        colIntuple = 0;
        tupleInPgae = 0;
        tupleList = new ArrayList<>();
        tupleList.add(0);


    }


    /**
     * Function: close
     * close reader files and associated bufferchannels
     * @throws IOException
     */
    public void close() throws IOException
    {
        page.clear();
        fc.close();
    }

    /**
     * reset method to reset relation by a tuple index
     * @param index
     * @throws Exception
     */

    public void reset(int index) throws Exception{
        int pageNum = Collections.binarySearch(tupleList, index + 1);
        //System.out.println("the pageIdx is: " + pageNum);
        pageNum = pageNum >= 0 ? pageNum : -(pageNum + 1);

        fc.position((pageNum - 1) * size);
        setZeros();
        newPage = true;
        eof = false;
        tupleList = tupleList.subList(0, pageNum);
        int tupleInList= tupleList.get(tupleList.size() - 1);
        int pos = (index - tupleInList) * colIntuple * 4 + 8;
        page.clear();
        if(fc.read(page) < 0) return;
        setPage();
        page.position(pos);
    }
}
