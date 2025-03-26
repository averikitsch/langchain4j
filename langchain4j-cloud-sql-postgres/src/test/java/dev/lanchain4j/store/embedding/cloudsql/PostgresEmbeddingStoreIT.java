package dev.langchain4j.store.embedding.cloudsql;

import static dev.langchain4j.utils.CloudsqlTestUtils.randomPGvector;
import static org.assertj.core.api.Assertions.assertThat;

import com.pgvector.PGvector;
import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.engine.EmbeddingStoreConfig;
import dev.langchain4j.engine.MetadataColumn;
import dev.langchain4j.engine.PostgresEngine;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.filter.comparison.IsIn;
import dev.langchain4j.store.embedding.index.DistanceStrategy;
import java.sql.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
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
    private static String iamEmail;

    private static PostgresEngine engine;
    private static PostgresEmbeddingStore store;
    private static Connection defaultConnection;
    private static EmbeddingStoreConfig embeddingStoreConfig;
    private static final String TABLE_NAME = "JAVA_EMBEDDING_TEST_TABLE";
    private static final Integer VECTOR_SIZE = 100;
    private static final String ipType = "public";

    @BeforeAll
    public static void beforeAll() throws SQLException {
        projectId = System.getenv("POSTGRES_PROJECT_ID");
        region = System.getenv("REGION");
        cluster = System.getenv("POSTGRES_CLUSTER");
        instance = System.getenv("POSTGRES_INSTANCE");
        database = System.getenv("POSTGRES_DB");
        user = System.getenv("POSTGRES_USER");
        password = System.getenv("POSTGRES_PASS");
        iamEmail = System.getenv("POSTGRES_IAM_EMAIL");

        engine = new PostgresEngine.Builder()
                .projectId(projectId)
                .region(region)
                .instance(instance)
                .database(database)
                .user(user)
                .password(password)
                .ipType(ipType)
                .iamAccountEmail(iamEmail)
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

    @AfterEach
    public void afterEach() throws SQLException {
        defaultConnection.createStatement().executeUpdate(String.format("TRUNCATE TABLE \"%s\"", TABLE_NAME));
    }

    @AfterAll
    public static void afterAll() throws SQLException {
        defaultConnection.createStatement().executeUpdate(String.format("DROP TABLE IF EXISTS \"%s\"", TABLE_NAME));
        defaultConnection.close();
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

    @Test
    void search_for_vector_min_score_0() {
        List<Embedding> embeddings = new ArrayList<>();
        List<TextSegment> textSegments = new ArrayList<>();
        Map<Integer, Map<String, Object>> metaMaps = new HashMap<>();

        Stack<String> hayStack = new Stack<>();
        for (int i = 0; i < 10; i++) {
            PGvector vector = randomPGvector(VECTOR_SIZE);
            embeddings.add(new Embedding(vector.toArray()));
            Map<String, Object> metaMap = new HashMap<>();
            metaMap.put("string", "s" + i);
            metaMap.put("uuid", UUID.randomUUID());
            metaMap.put("integer", i);
            metaMap.put("long", 1L);
            metaMap.put("float", 1f);
            metaMap.put("double", 1d);
            metaMap.put("extra", "not in table columns " + i);
            metaMap.put("extra_credits", 100 + i);
            Metadata metadata = new Metadata(metaMap);
            textSegments.add(new TextSegment("this is a test text " + i, metadata));
            metaMaps.put(i, metaMap);

            hayStack.push("s" + i);
        }

        store.addAll(embeddings, textSegments);

        // filter by a column
        IsIn isIn = new IsIn("string", hayStack);

        EmbeddingSearchRequest request = EmbeddingSearchRequest.builder()
                .queryEmbedding(embeddings.get(1))
                .maxResults(10)
                .minScore(0.0)
                .filter(isIn)
                .build();

        List<EmbeddingMatch<TextSegment>> result = store.search(request).matches();

        // should return all 10
        assertThat(result.size()).isEqualTo(10);

        for (EmbeddingMatch<TextSegment> match : result) {
            Map<String, Object> matchMetadata = match.embedded().metadata().toMap();
            Integer index = (Integer) matchMetadata.get("integer");
            assertThat(match.embedded().text()).contains("this is a test text " + index);
            // metadata json should be unpacked into the original columns
            for (String column : matchMetadata.keySet()) {
                assertThat(matchMetadata.get(column))
                        .isEqualTo(metaMaps.get(index).get(column));
            }
        }
    }
}
