package dev.langchain4j.store.embedding.alloydb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import static dev.langchain4j.internal.Utils.isNotNullOrBlank;
import static dev.langchain4j.internal.Utils.isNullOrBlank;
import static dev.langchain4j.internal.Utils.readBytes;
import static dev.langchain4j.internal.ValidationUtils.ensureGreaterThanZero;
import static dev.langchain4j.internal.ValidationUtils.ensureNotBlank;
import dev.langchain4j.store.embedding.alloydb.index.HNSWIndex;
import dev.langchain4j.store.embedding.alloydb.index.VectorIndex;

public class AlloyDBEngine {

    private static final Logger log = Logger.getLogger(AlloyDBEngine.class.getName());
    private DataSource dataSource;

    /**
     * Constructor for AlloyDBEngine
     *
     * @param projectId (Required) AlloyDB project id
     * @param region (Required) AlloyDB cluster region
     * @param cluster (Required) AlloyDB cluster
     * @param instance (Required) AlloyDB instance
     * @param database (Required) AlloyDB database
     * @param user (Optional) AlloyDB database user
     * @param password (Optional) AlloyDB database password
     * @param ipType (Required) type of IP to be used (PUBLIC, PSC)
     * @param iamAccountEmail (Optional) IAM account email
     */
    public AlloyDBEngine(
            String projectId,
            String region,
            String cluster,
            String instance,
            String database,
            String user,
            String password,
            String ipType,
            String iamAccountEmail
    ) {
        Boolean enableIAMAuth;
        if (isNotNullOrBlank(user) && isNotNullOrBlank(password)) {
            enableIAMAuth = false;
        } else {
            enableIAMAuth = true;
            if (isNotNullOrBlank(iamAccountEmail)) {
                user = iamAccountEmail;
            } else {
                user = getIAMPrincipalEmail().replace(".gserviceaccount.com", "");
            }
        }
        String instanceName = new StringBuilder("projects/").append(ensureNotBlank(projectId, "projectId")).append("/locations/")
                .append(ensureNotBlank(region, "region")).append("/clusters/").append(ensureNotBlank(cluster, "cluster")).append("/instances/").append(ensureNotBlank(instance, "instance")).toString();
        dataSource = createDataSource(database, user, password, instanceName, ipType, enableIAMAuth);
    }

    private HikariDataSource createDataSource(
            String database,
            String user,
            String password,
            String instanceName,
            String ipType,
            Boolean enableIAMAuth
    ) {
        HikariConfig config = new HikariConfig();
        config.setUsername(ensureNotBlank(user, "user"));
        if (enableIAMAuth) {
            config.addDataSourceProperty("alloydbEnableIAMAuth", "true");
        } else {
            config.setPassword(ensureNotBlank(password, "password"));
        }
        config.setJdbcUrl(String.format("jdbc:postgresql:///%s", ensureNotBlank(database, "database")));
        config.addDataSourceProperty("socketFactory", "com.google.cloud.alloydb.SocketFactory");
        config.addDataSourceProperty("alloydbInstanceName", ensureNotBlank(instanceName, "instanceName"));
        config.addDataSourceProperty("alloydbIpType", ensureNotBlank(ipType, "ipType"));

        return new HikariDataSource(config);
    }

    private String getIAMPrincipalEmail() {
        try {
            GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
            String accessToken = credentials.refreshAccessToken().getTokenValue();

            String oauth2APIURL = "https://oauth2.googleapis.com/tokeninfo?access_token=" + accessToken;
            byte[] responseBytes = readBytes(oauth2APIURL);
            JsonObject responseJson = JsonParser.parseString(new String(responseBytes)).getAsJsonObject();
            if (responseJson.has("email")) {
                return responseJson.get("email").getAsString();
            } else {
                throw new RuntimeException("unable to load IAM principal email");
            }
        } catch (IOException e) {
            throw new RuntimeException("unable to load IAM principal email", e);
        }
    }

    public Connection getConnection() throws SQLException {
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        statement.executeUpdate("CREATE EXTENSION IF NOT EXISTS vector");
        return connection;
    }

    /**
     * create a non-default VectorStore table
     *
     * @param tableName (Required) the table name to create - does not append a
     * suffix or prefix!
     * @param vectorSize (Required) create a vector column with custom vector
     * size
     * @param contentColumn (Default: "content") create the content column with
     * custom name
     * @param embeddingColumn (Default: "embedding") create the embedding column
     * with custom name
     * @param metadataColumns (Default: "metadata") list of SQLAlchemy Columns
     * to create for custom metadata
     * @param indexType (Default: HNSWIndex) set the index type, supported
     * types: HNSWIndex, IVFFLATIndex, KNNIndex
     * @param overwriteExisting (Default: False) boolean for dropping table
     * before insertion
     * @param storeMetadata (Default: True) boolean to store extra metadata in
     * metadata column if not described in “metadata” field list
     */
    public void initVectorStoreTable(String tableName, Integer vectoreSize, String contentColumn, String embeddingColumn, List<MetadataColumn> metadataColumns, VectorIndex vectorIndex, Boolean overwriteExisting, Boolean storeMetadata) {
        Connection connection;
        ensureNotBlank(tableName, "tableName");

        try {
            connection = getConnection();
            Statement statement = connection.createStatement();
            if (overwriteExisting) {
                statement.executeUpdate(String.format("DROP TABLE %s", tableName));
            }
            if (isNullOrBlank(contentColumn)) {
                contentColumn = "content";
            }
            if (isNullOrBlank(embeddingColumn)) {
                embeddingColumn = "embedding";
            }
            String metadataClause = "";
            if (metadataColumns != null && !metadataColumns.isEmpty()) {
                if (!storeMetadata) {
                    throw new IllegalStateException("storeMetadata option is disabled but metadata was provided");
                }
                metadataClause = metadataColumns.stream().map(MetadataColumn::generateColumnString).collect(Collectors.joining(","));
            } else if (storeMetadata) {
                throw new IllegalStateException("storeMetadata option is enabled but no metadata was provided");
            }
            String query = String.format("CREATE TABLE IF NOT EXISTS %s (embedding_id UUID PRIMARY KEY, %s TEXT, %s vector(%d) NOT NULL, %s)", tableName,
                    contentColumn, embeddingColumn, ensureGreaterThanZero(vectoreSize, "vectoreSize"), metadataClause);
            statement.executeUpdate(query);
            if (vectorIndex == null) {
                // default index
                vectorIndex = new HNSWIndex(tableName, contentColumn, null, null, null, null, null);
            }
            query = vectorIndex.generateCreateIndexQuery();
            statement.executeUpdate(query);
        } catch (SQLException ex) {
            throw new RuntimeException(String.format("Failed to initialize vector store table: %s", tableName), ex);
        }
    }

    public void initChatHistoryTable() {
        //to be implemented
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String projectId;
        private String region;
        private String cluster;
        private String instance;
        private String database;
        private String user;
        private String password;
        private String ipType;
        private String iamAccountEmail;

        public Builder() {
        }

        public Builder projectId(String projectId) {
            this.projectId = projectId;
            return this;
        }

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        public Builder cluster(String cluster) {
            this.cluster = cluster;
            return this;
        }

        public Builder instance(String instance) {
            this.instance = instance;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder user(String user) {
            this.user = user;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder ipType(String ipType) {
            this.ipType = ipType;
            return this;
        }

        public Builder iamAccountEmail(String iamAccountEmail) {
            this.iamAccountEmail = iamAccountEmail;
            return this;
        }

        public AlloyDBEngine build() {
            return new AlloyDBEngine(projectId, region, cluster, instance, database, user, password, ipType, iamAccountEmail);
        }
    }
}
