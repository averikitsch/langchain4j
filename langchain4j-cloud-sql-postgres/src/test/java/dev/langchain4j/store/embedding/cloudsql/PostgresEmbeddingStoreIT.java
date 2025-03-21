package dev.langchain4j.store.embedding.cloudsql;

import static dev.langchain4j.utils.AlloyDBTestUtils.randomPGvector;
import static org.assertj.core.api.Assertions.assertThat;

import com.pgvector.PGvector;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.engine.EmbeddingStoreConfig;
import dev.langchain4j.engine.MetadataColumn;
import dev.langchain4j.engine.PostgresEngine;
import dev.langchain4j.store.embedding.index.DistanceStrategy;
import java.sql.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class PostgresEmbeddingStoreIT {

    private static String projectId;
    private static String region;
    private static String cluster;
    private static String instance;
    private static String database;
    private static String user;
    private static String password;

    private static PostgresEngine engine;
    private static PostgresEmbeddingStore store;
    private static Connection defaultConnection;
    private static EmbeddingStoreConfig embeddingStoreConfig;
    private static final String TABLE_NAME = "JAVA_EMBEDDING_TEST_TABLE";
    private static final Integer VECTOR_SIZE = 384;

    @BeforeAll
    public static void beforeAll() throws SQLException {

        projectId = System.getenv("POSTGRES_PROJECT_ID");
        region = System.getenv("REGION");
        cluster = System.getenv("POSTGRES_CLUSTER");
        instance = System.getenv("POSTGRES_INSTANCE");
        database = System.getenv("POSTGRES_DB");
        user = System.getenv("POSTGRES_USER");
        password = System.getenv("POSTGRES_PASS");
        // IAM_EMAIL = System.getenv("POSTGRES_IAM_EMAIL");

        engine = new PostgresEngine.Builder()
                //     .host("127.0.0.1")
                //      .port(5432)
                .database(database)
                .user(user)
                .password(password)
                .build();

        List<MetadataColumn> metadataColumns = new ArrayList<>();
        metadataColumns.add(new MetadataColumn("string", "text", true));
        metadataColumns.add(new MetadataColumn("uuid", "uuid", true));
        metadataColumns.add(new MetadataColumn("integer", "integer", true));
        metadataColumns.add(new MetadataColumn("long", "bigint", true));
        metadataColumns.add(new MetadataColumn("float", "real", true));
        metadataColumns.add(new MetadataColumn("double", "double precision", true));

        embeddingStoreConfig = new EmbeddingStoreConfig.Builder(TABLE_NAME, VECTOR_SIZE)
                .metadataColumns(metadataColumns)
                .storeMetadata(true)
                .build();

        defaultConnection = engine.getConnection();

        defaultConnection.createStatement().executeUpdate(String.format("DROP TABLE IF EXISTS \"%s\"", TABLE_NAME));

        engine.initVectorStoreTable(embeddingStoreConfig);

        List<String> metaColumnNames =
                metadataColumns.stream().map(c -> c.getName()).collect(Collectors.toList());

        store = new PostgresEmbeddingStore.Builder(engine, TABLE_NAME)
                .distanceStrategy(DistanceStrategy.COSINE_DISTANCE)
                .metadataColumns(metaColumnNames)
                .build();
    }

    @Test
    void remove_all_from_store() throws SQLException {
        List<Embedding> embeddings = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            PGvector vector = randomPGvector(VECTOR_SIZE);
            embeddings.add(new Embedding(vector.toArray()));
        }
        List<String> ids = store.addAll(embeddings);
        String stringIds = ids.stream().map(id -> String.format("'%s'", id)).collect(Collectors.joining(","));
        try (Statement statement = defaultConnection.createStatement(); ) {
            // assert IDs exist
            ResultSet rs = statement.executeQuery(String.format(
                    "SELECT \"%s\" FROM \"%s\" WHERE \"%s\" IN (%s)",
                    embeddingStoreConfig.getIdColumn(), TABLE_NAME, embeddingStoreConfig.getIdColumn(), stringIds));
            while (rs.next()) {
                String response = rs.getString(embeddingStoreConfig.getIdColumn());
                assertThat(ids).contains(response);
            }
        }

        store.removeAll(ids);

        try (Statement statement = defaultConnection.createStatement(); ) {
            // assert IDs were removed
            ResultSet rs = statement.executeQuery(String.format(
                    "SELECT \"%s\" FROM \"%s\" WHERE \"%s\" IN (%s)",
                    embeddingStoreConfig.getIdColumn(), TABLE_NAME, embeddingStoreConfig.getIdColumn(), stringIds));
            assertThat(rs.isBeforeFirst()).isFalse();
        }
    }
}
