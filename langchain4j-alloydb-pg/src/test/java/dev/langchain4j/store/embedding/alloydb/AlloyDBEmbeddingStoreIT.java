package dev.langchain4j.store.embedding.alloydb;

import static org.testcontainers.shaded.org.apache.commons.lang3.RandomUtils.nextInt;

import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.engine.AlloyDBEngine;
import dev.langchain4j.engine.EmbeddingStoreConfig;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.embedding.onnx.allminilml6v2q.AllMiniLmL6V2QuantizedEmbeddingModel;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.EmbeddingStoreWithFilteringIT;
import dev.langchain4j.store.embedding.index.DistanceStrategy;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class AlloyDBEmbeddingStoreIT extends EmbeddingStoreWithFilteringIT {

    @Container
    static PostgreSQLContainer<?> pgVector = new PostgreSQLContainer<>("pgvector/pgvector:pg15");

    final String tableName = "test" + nextInt(2000, 3000);
    AlloyDBEngine engine;
    EmbeddingStore<TextSegment> embeddingStore;
    EmbeddingModel embeddingModel = new AllMiniLmL6V2QuantizedEmbeddingModel();

    @Override
    protected void ensureStoreIsReady() {
        engine = new AlloyDBEngine.Builder()
                .host(pgVector.getHost())
                .port(pgVector.getFirstMappedPort())
                .user("test")
                .password("test")
                .database("test")
                .build();

        engine.initVectorStoreTable(new EmbeddingStoreConfig.Builder(tableName, 384).build());

        embeddingStore = new AlloyDBEmbeddingStore.Builder(engine, tableName)
                .distanceStrategy(DistanceStrategy.COSINE_DISTANCE)
                // .metadataColumns(metaColumnNames)
                .build();
    }

    @Override
    protected EmbeddingStore<TextSegment> embeddingStore() {
        return embeddingStore;
    }

    @Override
    protected EmbeddingModel embeddingModel() {
        return embeddingModel;
    }

    @Override
    protected boolean supportsContains() {
        return true;
    }

    // private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().enable(INDENT_OUTPUT);
    // private static final String TABLE_NAME = "JAVA_EMBEDDING_TEST_TABLE";
    // private static final Integer VECTOR_SIZE = 384;
    // private static final EmbeddingModel embeddingModel = new AllMiniLmL6V2QuantizedEmbeddingModel();
    // private static EmbeddingStoreConfig embeddingStoreConfig;
    // private static String projectId;
    // private static String region;
    // private static String cluster;
    // private static String instance;
    // private static String database;
    // private static String user;
    // private static String password;

    // private static AlloyDBEngine engine;
    // private static AlloyDBEmbeddingStore store;
    // private static Connection defaultConnection;

    // @BeforeAll
    // public static void beforeAll() throws SQLException {
    //     projectId = System.getenv("ALLOYDB_PROJECT_ID");
    //     region = System.getenv("ALLOYDB_REGION");
    //     cluster = System.getenv("ALLOYDB_CLUSTER");
    //     instance = System.getenv("ALLOYDB_INSTANCE");
    //     database = System.getenv("ALLOYDB_DB_NAME");
    //     user = System.getenv("ALLOYDB_USER");
    //     password = System.getenv("ALLOYDB_PASSWORD");

    //     List<MetadataColumn> metadataColumns = new ArrayList<>();
    //     metadataColumns.add(new MetadataColumn("string", "text", true));
    //     metadataColumns.add(new MetadataColumn("uuid", "uuid", true));
    //     metadataColumns.add(new MetadataColumn("integer", "integer", true));
    //     metadataColumns.add(new MetadataColumn("long", "bigint", true));
    //     metadataColumns.add(new MetadataColumn("float", "real", true));
    //     metadataColumns.add(new MetadataColumn("double", "double precision", true));

    //     embeddingStoreConfig = new EmbeddingStoreConfig.Builder(TABLE_NAME, VECTOR_SIZE)
    //             .metadataColumns(metadataColumns)
    //             .storeMetadata(true)
    //             .build();

    //     defaultConnection = engine.getConnection();

    //     defaultConnection.createStatement().executeUpdate(String.format("DROP TABLE IF EXISTS \"%s\"", TABLE_NAME));

    //     engine.initVectorStoreTable(embeddingStoreConfig);

    //     List<String> metaColumnNames =
    //             metadataColumns.stream().map(c -> c.getName()).collect(Collectors.toList());

}
