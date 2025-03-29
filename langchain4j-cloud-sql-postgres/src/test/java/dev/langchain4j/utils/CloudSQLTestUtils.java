package dev.langchain4j.utils;

import static org.assertj.core.api.Assertions.assertThat;

import com.pgvector.PGvector;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class CloudSQLTestUtils {
    private static final Random RANDOM = new Random();

    public static PGvector randomPGvector(int length) {
        float[] vector = new float[length];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = RANDOM.nextFloat() * 1000;
        }
        return new PGvector(vector);
    }

    public static void verifyColumns(
            Connection connection, String schemaName, String tableName, Set<String> expectedColumns)
            throws SQLException {
        Set<String> actualNames = new HashSet<>();

        ResultSet resultSet = connection
                .createStatement()
                .executeQuery(String.format("SELECT * FROM \"%s\".\"%s\"", schemaName, tableName));
        ResultSetMetaData rsMeta = resultSet.getMetaData();
        int columnCount = rsMeta.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            actualNames.add(rsMeta.getColumnName(i));
        }
        assertThat(actualNames).isEqualTo(expectedColumns);
    }
}
