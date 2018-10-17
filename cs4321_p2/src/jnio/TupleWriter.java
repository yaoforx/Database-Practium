package jnio;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import util.Tuple;

public class TupleWriter {
    private static int size = 4096;
    private java.nio.channels.FileChannel fc;
    private ByteBuffer page;
    private int colInTuple;
    private FileOutputStream fo;
    boolean hasHeader = false;
    private int fitInPage;
    private int curIndex;

    public TupleWriter(String outputPath) throws FileNotFoundException {
        java.io.File file = new java.io.File(outputPath);
        fo = new FileOutputStream(file, false);
        fc = fo.getChannel();

        page = ByteBuffer.allocate(size);
        fitInPage = 0;
        curIndex = 0;
    }

    public void write(Tuple tuple) throws IOException {
        colInTuple = tuple.getSize();
        if (!hasHeader) {
            page.putInt(tuple.getSize());
            page.putInt(0);
            hasHeader = true;
            fitInPage = ((size - 8) / (4 * tuple.getSize()));
        }
        if (curIndex < fitInPage) {
            for (int i = 0; i < tuple.getSize(); i++) {
                page.putInt(tuple.getValue(i));
            }
            curIndex += 1;
        } else if(fitInPage == 0) {
            page = ByteBuffer.allocate(0);

        }
        else {
            while (page.hasRemaining()) page.putInt(0);
            page.putInt(4, curIndex);
            page.flip();
            fc.write(page);
            setPage();
            write(tuple);
        }
    }




    private void setPage() {
        curIndex = 0;
        hasHeader = false;
        page.clear();
        page.put(new byte[size]);
        page.clear();
    }

    public void close() throws IOException {
        while (page.hasRemaining()) page.putInt(0);
        if(fitInPage == 0) page.limit(0);
        if(page.limit() > 0)
            page.putInt(4, curIndex);
        page.flip();
        fc.write(page);
        fc.close();
        fo.close();
    }
}
