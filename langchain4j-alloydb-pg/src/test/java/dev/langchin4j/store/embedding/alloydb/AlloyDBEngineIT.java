package dev.langchin4j.store.embedding.alloydb;

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

import dev.langchain4j.store.embedding.alloydb.AlloyDBEngine;
import dev.langchain4j.store.embedding.alloydb.MetadataColumn;
import dev.langchain4j.store.embedding.alloydb.index.IVFFlatIndex;

public class AlloyDBEngineIT {

    private static final String TABLE_NAME = "JAVA_ENGINE_TEST_TABLE";
    private static final String CUSTOM_TABLE_NAME = "JAVA_ENGINE_TEST_CUSTOM_TABLE";
    private static final Integer VECTOR_SIZE = 768;
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

    }

    @AfterEach
    public void afterEach() throws SQLException {
        defaultConnection.createStatement().executeUpdate(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
        defaultConnection.createStatement().executeUpdate(String.format("DROP TABLE IF EXISTS %s", CUSTOM_TABLE_NAME));
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

    private void verifyIndex(String tableName, String type, String expected) throws SQLException {
        try (Connection connection = engine.getConnection()) {
            ResultSet indexes = connection.createStatement().executeQuery(String.format("SELECT indexdef FROM pg_indexes WHERE tablename = '%s' AND indexname = '%s_%s_index'", tableName.toLowerCase(), tableName.toLowerCase(), type));
            while (indexes.next()) {
                assertThat(indexes.getString("indexdef")).contains(expected);
            }
        }
    }

    @Test
    void initialize_vector_table_with_default_schema() throws SQLException {
        // default options
        engine.initVectorStoreTable(TABLE_NAME, VECTOR_SIZE, null, null, null, null, null, null, false);

        Set<String> expectedNames = new HashSet<>();

        expectedNames.add("langchain_id");
        expectedNames.add("content");
        expectedNames.add("embedding");

        verifyColumns(TABLE_NAME, expectedNames);

        verifyIndex(TABLE_NAME, "hnsw", "USING hnsw (embedding vector_l2_ops)");

    }

    @Test
    void initialize_vector_table_overwrite_true() throws SQLException {
        // default options
        engine.initVectorStoreTable(TABLE_NAME, VECTOR_SIZE, null, null, null, null, null, null, false);
        // custom
        engine.initVectorStoreTable(TABLE_NAME, VECTOR_SIZE, "overwritten", null, null, null, null, true, false);

        Set<String> expectedColumns = new HashSet<>();
        expectedColumns.add("langchain_id");
        expectedColumns.add("overwritten");
        expectedColumns.add("embedding");

        verifyColumns(TABLE_NAME, expectedColumns);

    }

    @Test
    void initialize_vector_table_with_custom_options() throws SQLException {
        List<MetadataColumn> metadataColumns = new ArrayList<>();
        metadataColumns.add(new MetadataColumn("page", "TEXT", true));
        metadataColumns.add(new MetadataColumn("source", "TEXT", false));
        engine.initVectorStoreTable(CUSTOM_TABLE_NAME, 1000, "custom_content_column", "custom_embedding_column", "custom_embedding_id_column", metadataColumns, new IVFFlatIndex(CUSTOM_TABLE_NAME, "custom_embedding_column", null, null, null, null), false, true);

        Set<String> expectedColumns = new HashSet<>();
        expectedColumns.add("custom_embedding_id_column");
        expectedColumns.add("custom_content_column");
        expectedColumns.add("custom_embedding_column");
        expectedColumns.add("page");
        expectedColumns.add("source");

        verifyColumns(CUSTOM_TABLE_NAME, expectedColumns);

        verifyIndex(CUSTOM_TABLE_NAME, "ivfflat", "USING ivfflat (custom_embedding_column)");

    }

    @Test
    void create_from_existing_fails_if_table_not_present() {

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            engine.initVectorStoreTable(TABLE_NAME, VECTOR_SIZE, null, null, null, null, null, true, false);
        });

        assertThat(exception.getMessage()).contains("Failed to initialize vector store table: " + TABLE_NAME);

    }

    @Test
    void create_fails_when_table_present_and_overwrite_false() {
        engine.initVectorStoreTable(TABLE_NAME, VECTOR_SIZE, null, null, null, null, null, false, false);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            engine.initVectorStoreTable(TABLE_NAME, VECTOR_SIZE, null, null, null, null, null, false, false);
        });

        assertThat(exception.getMessage()).contains(String.format("Overwrite option is false but table %s is present", TABLE_NAME));

    }

    @Test
    void table_create_fails_when_metadata_present_and_ignore_metadata_true() {
        List<MetadataColumn> metadataColumns = new ArrayList<>();
        metadataColumns.add(new MetadataColumn("page", "TEXT", true));
        metadataColumns.add(new MetadataColumn("source", "TEXT", true));

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            engine.initVectorStoreTable(TABLE_NAME, VECTOR_SIZE, "custom_content_column", "custom_embedding_column", "custom_embedding_id_column", metadataColumns, null, false, false);
        });

        assertThat(exception.getMessage()).contains("storeMetadata option is disabled but metadata was provided");
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
