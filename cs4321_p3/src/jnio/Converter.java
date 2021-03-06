package jnio;


import org.junit.Test;
import util.Tuple;
import static org.junit.Assert.*;

import java.io.*;
//import org.apache.commons.io.*;
/**
 * Utils Class for converting binary to humanreadable
 */
public class Converter {
    private TupleReader reader;
    private BufferedWriter writer;
    private TupleWriter bwriter;
   // private NormalTupleReader breader;
    String input = "/Users/yaoxiao/Database-Practium/cs4321_p2/samples/input/db/data/";
    String output = "/Users/yaoxiao/Database-Practium/cs4321_p2/samples/input/db/data/";
    File in;
    public Converter (String path) throws IOException {
        in  = new File(path + "/");
        writer = new BufferedWriter(new FileWriter(path  +  "_humanreadable"));
       // binaryToReadable();
    }
    public void binaryToReadable() {
        Tuple tp;

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
//    public void ReadableToBinary() throws IOException {
//        NormalTupleReader reader = new NormalTupleReader(input + "Reserves_humanreadable");
//        TupleWriter writer = new TupleWriter(output + "Reserves");
//        Tuple t;
//        while ((t = reader.read()) != null) {
//            System.out.println(t.toString());
//            writer.write(t);
//        }
//        reader.close();
//        writer.close();
//    }
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
//            System.out.println("testing " + i);
//            assertTrue(FileUtils.contentEquals(new File(target), new File(exp)));
//
//           assertTrue(FileUtils.contentEquals(new File(readble), new File(targetReadable)));
//        }
//    }

}
