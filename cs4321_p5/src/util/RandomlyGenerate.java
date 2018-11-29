package util;

import org.junit.Test;
import jnio.*;

import java.io.IOException;

/**
 * class to run the RandomGenerator
 */
public class RandomlyGenerate {
    @Test
    public void main() {
        RandomGenerator random = new RandomGenerator(100, 3, 50, "/Users/kylejohnson/Documents/CS4321/cs4321_p3/samples/input/db/data/Test");
        random.generate();
        try {
            Converter convert = new Converter("/Users/kylejohnson/Documents/CS4321/cs4321_p3/samples/input/db/data/Test2");
            convert.binaryToReadable();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
