package jnio;

import util.Tuple;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * TupleReader reads tuple page by page
 */

public class TupleReader {

    ByteBuffer page;
    private static final int size = 4096;
    private boolean eof;
    private int colIntuple;
    private int tupleInPgae;
    private File file;
    private FileChannel fc;
    private boolean newPage;
    private int currentSize;

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


                return new Tuple(tuple);
            }
            newPage = true;
            setZeros();
        }
        return null;
    }

    /**
     * Function: setPage
     * Set a new page whenever a previous page is exhausted
     */

    private void setPage() {
        page.flip();
        colIntuple = page.getInt();
        tupleInPgae = page.getInt();
        currentSize = (colIntuple * tupleInPgae * 4 + 8);
        page.limit(currentSize);

        newPage = false;
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




    public void reset() {}


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
}
