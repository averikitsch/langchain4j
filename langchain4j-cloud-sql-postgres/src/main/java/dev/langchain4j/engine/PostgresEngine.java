package dev.langchain4j.engine;

import static dev.langchain4j.internal.Utils.isNotNullOrBlank;
import static dev.langchain4j.internal.Utils.isNullOrBlank;
import static dev.langchain4j.internal.Utils.readBytes;
import static dev.langchain4j.internal.ValidationUtils.ensureNotBlank;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.pgvector.PGvector;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PostgresEngine
 *
 * <p>Instances of this store are created by configuring a builder: {@code PostgresEngine engine =
 * new PostgresEngine.Builder(projectId, region, cluster, instance, database).build(); } Uses
 * HikariCP as a DataSource. A connection pool that will avoid the latency of repeatedly creating
 * new database connections.
 */
public class PostgresEngine {

    private static final Logger log = LoggerFactory.getLogger(PostgresEngine.class.getName());

    private final HikariDataSource dataSource;

    /**
     * Constructor for PostgresEngine
     *
     * @param builder builder
     */
    public PostgresEngine(Builder builder) {
        Boolean enableIAMAuth = false;

        if (isNullOrBlank(builder.user) && isNullOrBlank(builder.password)) {
            enableIAMAuth = true;
            if (isNotNullOrBlank(builder.iamAccountEmail)) {
                log.debug("Found iamAccountEmail");
                builder.user = builder.iamAccountEmail;
            } else {
                log.debug("Retrieving IAM principal email");
                builder.user = getIAMPrincipalEmail().replace(".gserviceaccount.com", "");
            }
        } else if (isNotNullOrBlank(builder.user) && isNotNullOrBlank(builder.password)) {
            enableIAMAuth = false;
            log.debug("Found user and password, IAM Auth disabled");
        } else {
            throw new IllegalStateException(
                    "Either one of user or password is blank, expected both user and password to be valid"
                            + " credentials or empty");
        }
        String instanceName = new StringBuilder(ensureNotBlank(builder.projectId, "projectId"))
                .append(":")
                .append(ensureNotBlank(builder.region, "region"))
                .append(":")
                .append(ensureNotBlank(builder.instance, "instance"))
                .toString();
        dataSource = createDataSource(
                builder.database, builder.user, builder.password, instanceName, builder.ipType, enableIAMAuth);
    }

    private HikariDataSource createDataSource(
            String database, String user, String password, String instanceName, String ipType, Boolean enableIAMAuth) {
        HikariConfig config = new HikariConfig();
        config.setUsername(ensureNotBlank(user, "user"));
        if (enableIAMAuth) {
            config.addDataSourceProperty("enableIAMAuth", "true");
        } else {
            config.setPassword(ensureNotBlank(password, "password"));
        }
        config.setJdbcUrl(String.format("jdbc:postgresql:///%s", ensureNotBlank(database, "database")));
        config.addDataSourceProperty("socketFactoryArg", "com.google.cloud.postgres.SocketFactory");
        config.addDataSourceProperty("cloudSqlInstance", ensureNotBlank(instanceName, "instanceName"));
        config.addDataSourceProperty("ipType", ensureNotBlank(ipType, "ipType"));

        return new HikariDataSource(config);
    }

    private String getIAMPrincipalEmail() {
        try {
            GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
            String accessToken = credentials.refreshAccessToken().getTokenValue();

            String oauth2APIURL = "https://oauth2.googleapis.com/tokeninfo?access_token=" + accessToken;
            byte[] responseBytes = readBytes(oauth2APIURL);
            JsonObject responseJson =
                    JsonParser.parseString(new String(responseBytes)).getAsJsonObject();
            if (responseJson.has("email")) {
                return responseJson.get("email").getAsString();
            } else {
                throw new RuntimeException("unable to load IAM principal email");
            }
        } catch (IOException e) {
            throw new RuntimeException("unable to load IAM principal email", e);
        }
    }

    /**
     * Gets a Connection from the datasource
     *
     * @return A connection with the database specified in {@link PostgresEngine}
     * @throws SQLException if database error occurs
     */
    public Connection getConnection() throws SQLException {
        Connection connection = dataSource.getConnection();
        PGvector.addVectorType(connection);
        return connection;
    }

    /** Closes a Connection */
    public void close() {
        dataSource.close();
    }

    /**
     * builder
     *
     * @return builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder which configures and creates instances of {@link PostgresEngine}. Connect directly to
     * an instance using projectId, region, cluster, instance, and database params (Optional:
     * user/password, iamAccountEmail, ipType) or connect via an IP Address using host, user,
     * password, and database params (Optional: port)
     */
    public static class Builder {

        private String projectId;
        private String region;
        private String instance;
        private String database;
        private String user;
        private String password;
        private String ipType = "public";
        private String iamAccountEmail;

        /** Creates a new {@code Builder} instance. */
        public Builder() {}

        /**
         * project id
         *
         * @param projectId (Required) project id
         * @return this builder
         */
        public Builder projectId(String projectId) {
            this.projectId = projectId;
            return this;
        }

        /**
         * region
         *
         * @param region (Required) cluster region
         * @return this builder
         */
        public Builder region(String region) {
            this.region = region;
            return this;
        }

        /**
         * instance
         *
         * @param instance (Required)
         * @return this builder
         */
        public Builder instance(String instance) {
            this.instance = instance;
            return this;
        }

        /**
         * database
         *
         * @param database (Required)
         * @return this builder
         */
        public Builder database(String database) {
            this.database = database;
            return this;
        }

        /**
         * database user
         *
         * @param user (Required)
         * @return this builder
         */
        public Builder user(String user) {
            this.user = user;
            return this;
        }

        /**
         * database password
         *
         * @param password (Required)
         * @return this builder
         */
        public Builder password(String password) {
            this.password = password;
            return this;
        }

        /**
         * type of IP to be used (PUBLIC, PSC)
         *
         * @param ipType (Required)
         * @return this builder
         */
        public Builder ipType(String ipType) {
            this.ipType = ipType;
            return this;
        }

        /**
         * IAM account email
         *
         * @param iamAccountEmail (Optional)
         * @return this builder
         */
        public Builder iamAccountEmail(String iamAccountEmail) {
            this.iamAccountEmail = iamAccountEmail;
            return this;
        }

        /**
         * Builds an {@link PostgresEngine} store with the configuration applied to this builder.
         *
         * @return A new {@link PostgresEngine} instance
         */
        public PostgresEngine build() {
            return new PostgresEngine(this);
        }
    }

    /**
     * intialize the Vector table
     *
     * @param embeddingStoreConfig contains the parameters necesary to intialize the Vector table
     */
    public void initVectorStoreTable(EmbeddingStoreConfig embeddingStoreConfig) {

        try (Connection connection = getConnection(); ) {
            Statement statement = connection.createStatement();
            statement.executeUpdate("CREATE EXTENSION IF NOT EXISTS vector");

            if (embeddingStoreConfig.getOverwriteExisting()) {
                statement.executeUpdate(String.format(
                        "DROP TABLE \"%s\".\"%s\"",
                        embeddingStoreConfig.getSchemaName(), embeddingStoreConfig.getTableName()));
            }

            String metadataClause = "";
            if (embeddingStoreConfig.getMetadataColumns() != null
                    && !embeddingStoreConfig.getMetadataColumns().isEmpty()) {
                if (!embeddingStoreConfig.getStoreMetadata()) {
                    throw new IllegalStateException("storeMetadata option is disabled but metadata was provided");
                }
                metadataClause = String.format(
                        ", %s",
                        embeddingStoreConfig.getMetadataColumns().stream()
                                .map(MetadataColumn::generateColumnString)
                                .collect(Collectors.joining(",")));
            } else if (embeddingStoreConfig.getStoreMetadata()) {
                throw new IllegalStateException("storeMetadata option is enabled but no metadata was provided");
            }

            if (embeddingStoreConfig.getStoreMetadata()) {
                metadataClause += String.format(
                        ", %s",
                        new MetadataColumn(embeddingStoreConfig.getMetadataJsonColumn(), "JSON", true)
                                .generateColumnString());
            }

            String query = String.format(
                    "CREATE TABLE \"%s\".\"%s\" (\"%s\" UUID PRIMARY KEY, \"%s\" TEXT NULL, \"%s\""
                            + " vector(%d) NOT NULL%s)",
                    embeddingStoreConfig.getSchemaName(),
                    embeddingStoreConfig.getTableName(),
                    embeddingStoreConfig.getIdColumn(),
                    embeddingStoreConfig.getContentColumn(),
                    embeddingStoreConfig.getEmbeddingColumn(),
                    embeddingStoreConfig.getVectorSize(),
                    metadataClause);

            statement.executeUpdate(query);

        } catch (SQLException ex) {
            throw new RuntimeException(
                    String.format(
                            "Failed to initialize vector store table: \"%s\".\"%s\"",
                            embeddingStoreConfig.getSchemaName(), embeddingStoreConfig.getTableName()),
                    ex);
        }
    }
}
