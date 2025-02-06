package dev.langchain4j.engine;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import static dev.langchain4j.internal.Utils.isNotNullOrBlank;
import static dev.langchain4j.internal.Utils.isNullOrBlank;
import static dev.langchain4j.internal.Utils.readBytes;
import static dev.langchain4j.internal.ValidationUtils.ensureNotBlank;
import dev.langchain4j.engine.TableInitParameters;

public class AlloyDBEngine {

    private static final Logger log = LoggerFactory.getLogger(AlloyDBEngine.class.getName());
    private final DataSource dataSource;

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
        if (isNullOrBlank(user) && isNullOrBlank(password)) {
            enableIAMAuth = true;
            if (isNotNullOrBlank(iamAccountEmail)) {
                log.debug("Found iamAccountEmail");
                user = iamAccountEmail;
            } else {
                log.debug("Retrieving IAM principal email");
                user = getIAMPrincipalEmail().replace(".gserviceaccount.com", "");
            }
        } else if (isNotNullOrBlank(user) && isNotNullOrBlank(password)) {
            enableIAMAuth = false;
            log.debug("Found user and password, IAM Auth disabled");
        } else {
            throw new IllegalStateException("Either one of user or password is blank, expected both user and password to be valid credentials or empty");
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
        return connection;
    }


    /**
     * @param tableInitParameters contains the parameters necesary to intialize the Vector table 
     */
    public void initVectorStoreTable(TableInitParameters tableInitParameters) {
        try (Connection connection = getConnection();) {
            Statement statement = connection.createStatement();
            statement.executeUpdate("CREATE EXTENSION IF NOT EXISTS vector");
            statement.executeUpdate(String.format("CREATE SCHEMA IF NOT EXISTS %s", tableInitParameters.getSchemaName()));

            if (tableInitParameters.getOverwriteExisting()) {
                statement.executeUpdate(String.format("DROP TABLE %s.%s", tableInitParameters.getSchemaName(), tableInitParameters.getTableName()));
            }
            String metadataClause = "";
            if (tableInitParameters.getMetadataColumns() != null && !tableInitParameters.getMetadataColumns().isEmpty()) {
                if (!tableInitParameters.getStoreMetadata()) {
                    throw new IllegalStateException("storeMetadata option is disabled but metadata was provided");
                }
                metadataClause += String.format(", %s, %s", new MetadataColumn(tableInitParameters.getMetadataJsonColumn(), "JSON", true).generateColumnString(), tableInitParameters.getMetadataColumns().stream().map(MetadataColumn::generateColumnString).collect(Collectors.joining(",")));
            } else if (tableInitParameters.getStoreMetadata()) {
                throw new IllegalStateException("storeMetadata option is enabled but no metadata was provided");
            }
            String query = String.format("CREATE TABLE %s.%s (%s UUID PRIMARY KEY, %s TEXT, %s vector(%d) NOT NULL%s)", tableInitParameters.getSchemaName(), tableInitParameters.getTableName(), tableInitParameters.getEmbeddingIdColumn(),
            tableInitParameters.getContentColumn(), tableInitParameters.getEmbeddingColumn(), tableInitParameters.getVectorSize(), metadataClause);
            statement.executeUpdate(query);
        } catch (SQLException ex) {
            throw new RuntimeException(String.format("Failed to initialize vector store table: %s.%s", tableInitParameters.getSchemaName(), tableInitParameters.getTableName(), ex));
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

        /**
         * @param projectId (Required) AlloyDB project id
         */
        public Builder projectId(String projectId) {
            this.projectId = projectId;
            return this;
        }

        /**
         * @param region (Required) AlloyDB cluster region
         */
        public Builder region(String region) {
            this.region = region;
            return this;
        }

        /**
         * @param cluster (Required) AlloyDB cluster
         */
        public Builder cluster(String cluster) {
            this.cluster = cluster;
            return this;
        }

        /**
         * @param instance (Required) AlloyDB instance
         */
        public Builder instance(String instance) {
            this.instance = instance;
            return this;
        }

        /**
         * @param database (Required) AlloyDB database
         */
        public Builder database(String database) {
            this.database = database;
            return this;
        }

        /**
         * @param user (Optional) AlloyDB database user
         */
        public Builder user(String user) {
            this.user = user;
            return this;
        }

        /**
         * @param password (Optional) AlloyDB database password
         */
        public Builder password(String password) {
            this.password = password;
            return this;
        }

        /**
         * @param ipType (Required) type of IP to be used (PUBLIC, PSC)
         */
        public Builder ipType(String ipType) {
            this.ipType = ipType;
            return this;
        }

        /**
         * @param iamAccountEmail (Optional) IAM account email
         */
        public Builder iamAccountEmail(String iamAccountEmail) {
            this.iamAccountEmail = iamAccountEmail;
            return this;
        }

        public AlloyDBEngine build() {
            return new AlloyDBEngine(projectId, region, cluster, instance, database, user, password, ipType, iamAccountEmail);
        }
    }
}