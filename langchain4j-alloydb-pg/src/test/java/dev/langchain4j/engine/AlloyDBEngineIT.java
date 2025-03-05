package dev.langchain4j.engine;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static dev.langchain4j.internal.Utils.randomUUID;

public class AlloyDBEngineIT {

    private static final String TABLE_NAME = "java_engine_test_table" + randomUUID();
    private static final String CUSTOM_TABLE_NAME = "java_engine_test_custom_table" + randomUUID();
    private static final String CUSTOM_SCHEMA = "custom_schema";
    private static final Integer VECTOR_SIZE = 768;
    private static EmbeddingStoreConfig defaultParameters;
    private static String IAM_EMAIL;
    private static String projectId;
    private static String region;
    private static String cluster;
    private static String instance;
    private static String database;
    private static String user;
    private static String password;

    private static AlloyDBEngine engine;
    private static Connection defaultConnection;

    @BeforeAll
    public static void beforeAll() throws SQLException {
        projectId = System.getenv("ALLOYDB_PROJECT_ID");
        region = System.getenv("ALLOYDB_REGION");
        cluster = System.getenv("ALLOYDB_CLUSTER");
        instance = System.getenv("ALLOYDB_INSTANCE");
        database = System.getenv("ALLOYDB_DB_NAME");
        user = System.getenv("ALLOYDB_USER");
        password = System.getenv("ALLOYDB_PASSWORD");
        IAM_EMAIL = System.getenv("ALLOYDB_IAM_EMAIL");

        engine = AlloyDBEngine.builder().projectId(projectId).region(region).cluster(cluster).instance(instance).database(database).user(user).password(password).ipType("PUBLIC").build();

        defaultConnection = engine.getConnection();

        defaultConnection.createStatement().executeUpdate(String.format("CREATE SCHEMA IF NOT EXISTS \"%s\"", CUSTOM_SCHEMA));

        defaultParameters = EmbeddingStoreConfig.builder().tableName(TABLE_NAME).vectorSize(VECTOR_SIZE).build();

    }

    @AfterEach
    public void afterEach() throws SQLException {
        defaultConnection.createStatement().executeUpdate(String.format("DROP TABLE IF EXISTS \"%s\"", TABLE_NAME));
        defaultConnection.createStatement().executeUpdate(String.format("DROP TABLE IF EXISTS \"%s\"", CUSTOM_TABLE_NAME));
        defaultConnection.createStatement().executeUpdate(String.format("DROP TABLE IF EXISTS \"%s\".\"%s\"", CUSTOM_SCHEMA, TABLE_NAME));
        defaultConnection.createStatement().executeUpdate(String.format("DROP TABLE IF EXISTS \"%s\".\"%s\"", CUSTOM_SCHEMA, CUSTOM_TABLE_NAME));
    }

    @AfterAll
    public static void afterAll() throws SQLException {
        defaultConnection.close();
    }

    private void verifyColumns(String tableName, Set<String> expectedColumns) throws SQLException {
        Set<String> actualNames = new HashSet<>();

        try (ResultSet resultSet = engine.getConnection().createStatement().executeQuery("SELECT * FROM " + tableName)) {
            ResultSetMetaData rsMeta = resultSet.getMetaData();
            int columnCount = rsMeta.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                actualNames.add(rsMeta.getColumnName(i));
            }
            assertThat(actualNames).isEqualTo(expectedColumns);
        }
    }

    @Test
    void initialize_vector_table_with_default_schema() throws SQLException {
        // default options
        engine.initVectorStoreTable(defaultParameters);

        Set<String> expectedNames = new HashSet<>();

        expectedNames.add("langchain_id");
        expectedNames.add("content");
        expectedNames.add("embedding");

        verifyColumns(String.format("\"public\".\"%s\"", TABLE_NAME), expectedNames);

    }

    @Test
    void initialize_vector_table_overwrite_true() throws SQLException {
        // default options
        engine.initVectorStoreTable(defaultParameters);
        // custom
        EmbeddingStoreConfig overwritten = EmbeddingStoreConfig.builder().tableName(TABLE_NAME).vectorSize(VECTOR_SIZE).contentColumn("overwritten").overwriteExisting(true).build();
        engine.initVectorStoreTable(overwritten);

        Set<String> expectedColumns = new HashSet<>();
        expectedColumns.add("langchain_id");
        expectedColumns.add("overwritten");
        expectedColumns.add("embedding");

        verifyColumns(String.format("\"%s\"", TABLE_NAME), expectedColumns);

    }

    @Test
    void initialize_vector_table_with_custom_options() throws SQLException {
        List<MetadataColumn> metadataColumns = new ArrayList<>();
        metadataColumns.add(new MetadataColumn("page", "TEXT", true));
        metadataColumns.add(new MetadataColumn("source", "TEXT", false));

        EmbeddingStoreConfig customParams = EmbeddingStoreConfig.builder().tableName(CUSTOM_TABLE_NAME).vectorSize(1000).schemaName(CUSTOM_SCHEMA).contentColumn("custom_content_column")
                .embeddingColumn("custom_embedding_column").idColumn("custom_embedding_id_column").metadataColumns(metadataColumns).metadataJsonColumn("custom_metadata_json_column").overwriteExisting(false).storeMetadata(true).build();
        engine.initVectorStoreTable(customParams);
        Set<String> expectedColumns = new HashSet<>();
        expectedColumns.add("custom_embedding_id_column");
        expectedColumns.add("custom_content_column");
        expectedColumns.add("custom_embedding_column");
        expectedColumns.add("page");
        expectedColumns.add("source");
        expectedColumns.add("custom_metadata_json_column");

        verifyColumns(String.format("\"%s\".\"%s\"", CUSTOM_SCHEMA, CUSTOM_TABLE_NAME), expectedColumns);

    }

    @Test
    void create_from_existing_fails_if_table_not_present() {
        EmbeddingStoreConfig initParameters = EmbeddingStoreConfig.builder().tableName(TABLE_NAME).vectorSize(VECTOR_SIZE).overwriteExisting(true).storeMetadata(false).build();

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            engine.initVectorStoreTable(initParameters);
        });

        assertThat(exception.getMessage()).isEqualTo(String.format("Failed to initialize vector store table: \"public\".\"%s\"", TABLE_NAME));
        assertThat(exception.getCause().getMessage()).isEqualTo(String.format("ERROR: table \"%s\" does not exist", TABLE_NAME));
    }

    @Test
    void create_fails_when_table_present_and_overwrite_false() {
        EmbeddingStoreConfig initParameters = EmbeddingStoreConfig.builder().tableName(CUSTOM_TABLE_NAME).vectorSize(VECTOR_SIZE).storeMetadata(false).build();

        engine.initVectorStoreTable(initParameters);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            engine.initVectorStoreTable(initParameters);
        });

        assertThat(exception.getMessage()).isEqualTo(String.format("Failed to initialize vector store table: \"public\".\"%s\"", CUSTOM_TABLE_NAME));
        // table name is truncated in PSQL exception
        assertThat(exception.getCause().getMessage()).isEqualTo(String.format("ERROR: relation \"%s\" already exists", CUSTOM_TABLE_NAME.substring(0, CUSTOM_TABLE_NAME.length() - 2)));
    }

    @Test
    void create_engine_with_iam_auth() throws SQLException {
        AlloyDBEngine iam_engine = AlloyDBEngine.builder().projectId(projectId).region(region).cluster(cluster).instance(instance).database(database).ipType("PUBLIC").iamAccountEmail(IAM_EMAIL).build();
        try (Connection connection = iam_engine.getConnection();) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT 1");
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(1);
        }
    }

    @Test
    void create_engine_with_get_iam_email() throws SQLException {
        AlloyDBEngine iam_engine = AlloyDBEngine.builder().projectId(projectId).region(region).cluster(cluster).instance(instance).database(database).ipType("PUBLIC").build();
        try (Connection connection = iam_engine.getConnection();) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT 1");
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(1);
        }
    }

}
