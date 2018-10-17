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
    File in;
    public Converter (String fileName) throws IOException {
        in  = new File(input + fileName);
        writer = new BufferedWriter(new FileWriter(input + fileName + "_humanreadable"));
       // binaryToReadable();
    }
    public void binaryToReadable() {
        Tuple tp;

       // writer = new BufferedWriter(new FileWriter(in)))
        try {
            reader = new TupleReader(in);
            writer = new BufferedWriter(writer);
            while((tp = reader.read()) != null) {
                writer.write(tp.toString() + "\n");
            }
            writer.close();
            reader.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
//    @Test
//    public void exampleTest() throws IOException {
//        String expected = "/Users/yaoxiao/Documents/cs4321/cs4321_p2/samples/expected/";
//        for(int i = 1; i <= 15; i++) {
//            String target = input + "query" + i;
//            String exp = expected + "query" + i;
//           // binaryToReadable(target);
//            String readble = expected + "query" + i + "_humanreadable";
//            String targetReadable = input + "query" + i + "_humanreadable";
//            File tem1 = new File(target);
//            File tem2 = new File(exp);

//            assertTrue(FileUtils.contentEquals(new File(target), new File(exp)));

//           assertTrue(FileUtils.contentEquals(new File(readble), new File(targetReadable)));
//        }
//    }

}
