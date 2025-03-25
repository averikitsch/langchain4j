package dev.langchain4j.utils;

import com.pgvector.PGvector;
import java.util.Random;

public class CloudsqlTestUtils {
    private static final Random RANDOM = new Random();

    public static PGvector randomPGvector(int length) {
        float[] vector = new float[length];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = RANDOM.nextFloat() * 1000;
        }
        return new PGvector(vector);
    }
}
