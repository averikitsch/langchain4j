package dev.langchain4j.store.embedding.alloydb;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import static dev.langchain4j.internal.ValidationUtils.ensureNotBlank;

public class AlloyDBEngine {

    private static final Logger log = Logger.getLogger(AlloyDBEngine.class.getName());
    private DataSource dataSource;

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
        Boolean enableIAMAuth = false;
        if (user != null && !user.isBlank()) {
            if (password != null && !password.isBlank()) {
                enableIAMAuth = false;
            }
        } else {
            enableIAMAuth = true;
            if (iamAccountEmail != null && !iamAccountEmail.isBlank()) {
                user = iamAccountEmail;
            } else {
                // to be implemented
                user = getIAMPrincipalEmail();
            }
        }
        String instanceName = new StringBuilder("projects/").append(projectId).append("/locations/")
                .append(region).append("/clusters/").append(cluster).append("/instances/").append(instance).toString();
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
        config.setUsername(ensureNotBlank(user, "user")); // e.g., "postgres"
        if (enableIAMAuth) {
            config.addDataSourceProperty("alloydbEnableIAMAuth", "true");
        } else {
            config.setPassword(ensureNotBlank(password, "password")); // e.g., "secret-password"
        }
        config.setJdbcUrl(String.format("jdbc:postgresql:///%s", ensureNotBlank(database, "database")));
        config.addDataSourceProperty("socketFactory", "com.google.cloud.alloydb.SocketFactory");
        config.addDataSourceProperty("alloydbInstanceName", ensureNotBlank(instanceName, "instanceName"));
        config.addDataSourceProperty("alloydbIpType", ensureNotBlank(ipType, "ipType"));

        return new HikariDataSource(config);
    }

    private String getIAMPrincipalEmail() {
        //to be implemented
        return "";
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public void initVectorStoreTable() {
        //to be implemented
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
