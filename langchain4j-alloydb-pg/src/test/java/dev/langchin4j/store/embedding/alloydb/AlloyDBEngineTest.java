package dev.langchin4j.store.embedding.alloydb;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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

public class AlloyDBEngineTest {

    private static final String TABLE_NAME = "JAVA_ENGINE_TEST_TABLE";
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
        System.out.println(String.format("***%s, %s, %s", user, password, instance));

        engine = AlloyDBEngine.builder().projectId(projectId).region(region).cluster(cluster).instance(instance).database(database).user(user).password(password).ipType("PUBLIC").build();

        defaultConnection = engine.getConnection();

    }

    @AfterEach
    public void afterEach() throws SQLException {
        defaultConnection.createStatement().executeUpdate(String.format("DROP TABLE %s", TABLE_NAME));
    }

    @AfterAll
    public static void afterAll() throws SQLException {
        defaultConnection.close();
    }

    private void verifyColumns(String tablenName, Set<String> expectedColumns) throws SQLException {
        Set<String> actualNames = new HashSet<>();

        try (Connection connection = engine.getConnection(); ResultSet resultSet = connection.getMetaData().getColumns(null, connection.getSchema(), tablenName, "%")) {
            while (resultSet.next()) {
                assertThat(resultSet.getString("TABLE_NAME")).isEqualTo(tablenName);
                actualNames.add(resultSet.getString("COLUMN_NAME"));
            }
        }

        assertThat(actualNames).isEqualTo(expectedColumns);
    }

    @Test
    void initialize_vector_table_with_default_schema() throws SQLException {
        // default options
        engine.initVectorStoreTable(TABLE_NAME, VECTOR_SIZE, null, null, null, null, null, false);

        Set<String> expectedNames = new HashSet<>();

        expectedNames.add("embedding_id");
        expectedNames.add("content");
        expectedNames.add("embedding");

        verifyColumns(TABLE_NAME, expectedNames);

        Connection connection = engine.getConnection();
        DatabaseMetaData dbMetadata = connection.getMetaData();
        ResultSet indexes = dbMetadata.getIndexInfo(null, connection.getSchema(), TABLE_NAME, true, false);
        while (indexes.next()) {
            // TODO look for the name of hnsw
            System.out.println(indexes.getString("INDEX_NAME"));
        }
    }

    @Test
    void initialize_vector_table_overwrite_true() throws SQLException {
        // default options
        engine.initVectorStoreTable(TABLE_NAME, VECTOR_SIZE, null, null, null, null, null, false);
        engine.initVectorStoreTable("new_table_overwrite", VECTOR_SIZE, null, null, null, null, null, true);

        Connection connection = engine.getConnection();

        DatabaseMetaData dbMetadata = connection.getMetaData();
        ResultSet resultSet = dbMetadata.getColumns(null, connection.getSchema(), "new_table_overwrite", "%");
        assertThat(resultSet.getString("TABLE_NAME")).isEqualTo("new_table_overwrite");

        connection.createStatement().executeUpdate("DROP TABLE new_table_overwrite");

    }

    @Test
    void initialize_vector_table_with_custom_options() throws SQLException {
        List<MetadataColumn> metadataColumns = new ArrayList<>();
        metadataColumns.add(new MetadataColumn("page", "TEXT", true));
        metadataColumns.add(new MetadataColumn("source", "TEXT", true));

        engine.initVectorStoreTable("custom_table_name", 1000, "custom_content_column", "custom_embedding_column", metadataColumns, new IVFFlatIndex(TABLE_NAME, "custom_embedding_column", null, null, null, null), false, true);
        // metadata
        // embedding column
        // vector size

        /**
         * // TODO verify vector size SELECT attname AS column_name, atttypmod
         * - 4 AS vector_size FROM pg_attribute WHERE attrelid =
         * 'your_table_name'::regclass AND atttypid = 'vector'::regtype;
         */
        Set<String> expectedColumns = new HashSet<>();
        expectedColumns.add("custom_content_column");
        expectedColumns.add("custom_embedding_column");
        expectedColumns.add("page");
        expectedColumns.add("source");

        verifyColumns("custom_table_name", expectedColumns);

        Connection connection = engine.getConnection();
        Statement statement = connection.createStatement();

        ResultSet resultSet = statement.executeQuery("SELECT attname AS column_name, atttypmod - 4 AS vector_size FROM pg_attribute WHERE attrelid = 'your_table_name'::regclass AND atttypid = 'vector'::regtype");
        assertThat(resultSet.getInt("vector_size")).isEqualTo(1000);

        DatabaseMetaData dbMetadata = connection.getMetaData();
        ResultSet indexes = dbMetadata.getIndexInfo(null, connection.getSchema(), TABLE_NAME, true, false);
        while (indexes.next()) {
            // TODO look for the name of IVFFlatIndex
            System.out.println(indexes.getString("INDEX_NAME"));
        }
        connection.createStatement().executeUpdate("DROP TABLE custom_table_name");
    }

    @Test
    void initialize_vector_table_from_existing_table() throws SQLException {
        engine.initVectorStoreTable(TABLE_NAME, VECTOR_SIZE, null, null, null, null, null, false);

        List<MetadataColumn> metadataColumns = new ArrayList<>();
        metadataColumns.add(new MetadataColumn("page", "TEXT", true));
        metadataColumns.add(new MetadataColumn("source", "TEXT", true));

        engine.initVectorStoreTable(TABLE_NAME, VECTOR_SIZE, "custom_content_column", "custom_embedding_column", metadataColumns, new IVFFlatIndex(TABLE_NAME, "custom_embedding_column", null, null, null, null), true, true);
        // table should be updated
        Set<String> expectedColumns = new HashSet<>();
        expectedColumns.add("custom_content_column");
        expectedColumns.add("custom_embedding_column");
        expectedColumns.add("page");
        expectedColumns.add("source");

        verifyColumns("custom_table_name", expectedColumns);
    }

    @Test
    void create_from_existing_fails_if_table_not_present() {

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            engine.initVectorStoreTable(TABLE_NAME, VECTOR_SIZE, null, null, null, null, true, false);
        });

        assertThat(exception.getMessage()).contains("Failed to initialize vector store table: " + TABLE_NAME);

    }

    @Test
    void create_fails_when_table_present_and_overwrite_false() {
        engine.initVectorStoreTable(TABLE_NAME, VECTOR_SIZE, null, null, null, null, false, false);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            engine.initVectorStoreTable(TABLE_NAME, VECTOR_SIZE, null, null, null, null, false, false);
        });

        assertThat(exception.getMessage()).contains("Failed to initialize vector store table: " + TABLE_NAME);

    }

    @Test
    void table_create_fails_when_metadata_and_ignore_metadata() {
        List<MetadataColumn> metadataColumns = new ArrayList<>();
        metadataColumns.add(new MetadataColumn("page", "TEXT", true));
        metadataColumns.add(new MetadataColumn("source", "TEXT", true));

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            engine.initVectorStoreTable(TABLE_NAME, VECTOR_SIZE, "custom_content_column", "custom_embedding_column", metadataColumns, null, true, false);
        });

        assertThat(exception.getMessage()).contains("storeMetadata option is disabled but metadata was provided");
    }

    @Test
    void create_engine_with_iam_auth() throws SQLException {
        AlloyDBEngine iam_engine = AlloyDBEngine.builder().projectId(projectId).region(region).cluster(cluster).instance(instance).database(database).ipType("PUBLIC").iamAccountEmail(IAM_EMAIL).build();
        Connection connection = iam_engine.getConnection();
        assertThat(connection.createStatement().executeQuery("SELECT 1").getInt(1)).isEqualTo(1);
    }

    @Test
    void create_engine_with_get_iam_email() throws SQLException {
        AlloyDBEngine iam_engine = AlloyDBEngine.builder().projectId(projectId).region(region).cluster(cluster).instance(instance).database(database).ipType("PUBLIC").build();
        Connection connection = iam_engine.getConnection();
        assertThat(connection.createStatement().executeQuery("SELECT 1").getInt(1)).isEqualTo(1);
    }

}
