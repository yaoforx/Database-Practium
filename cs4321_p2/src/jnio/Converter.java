package jnio;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.junit.Test;
import util.Tuple;
import static org.junit.Assert.*;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Converter {
    private TupleReader reader;
    private BufferedWriter writer;
    String input = "/Users/yaoxiao/Documents/cs4321/cs4321_p2/";
    //String output = "";
   // File in;
    //File out;
    public Converter () throws IOException {
        //in  = new File(input);
        //writer = new BufferedWriter(new FileWriter(out));
    }
    public void binaryToReadable(String in) {
        Tuple tp;

       // writer = new BufferedWriter(new FileWriter(in)))
        try {
            File input = new File(in);
            File file  = new File(in.substring(0,in.length() - 4) + "_test.txt");
            reader = new TupleReader(input);

            writer = new BufferedWriter(new FileWriter(file));
            while((tp = reader.read()) != null) {
                writer.write(tp.toString() + "\n");
            }
            writer.close();
            reader.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //@Test
    public void exampleTest() throws IOException {
        String expected = "/Users/yaoxiao/Documents/cs4321/cs4321_p2/samples/expected/";
        for(int i = 1; i <= 16; i++) {
            String target = input + "query" + i + ".txt";
            String exp = expected + "query" + i;
           // binaryToReadable(target);

         //   assertTrue(FileUtils.contentEquals(new File(target), new File(exp)));
        }
    }
   // @Test
    public void exampleTestRead() throws IOException {
        String expected = "/Users/yaoxiao/Documents/cs4321/cs4321_p2/samples/expected/";
        for(int i = 1; i <= 1; i++) {
            String target = input + "query" + i + ".txt";
            String exp = expected + "query" + i + "_humanreadable";
            binaryToReadable(target);
            String readable = input + "query" + i +"_test.txt";

          //  assertTrue(FileUtils.contentEquals(new File(readable), new File(exp)));
        }
    }
}
