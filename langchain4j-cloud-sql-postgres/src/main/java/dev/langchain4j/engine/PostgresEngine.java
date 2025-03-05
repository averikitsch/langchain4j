package dev.langchain4j.engine;

import static dev.langchain4j.internal.Utils.isNotNullOrBlank;
import static dev.langchain4j.internal.Utils.isNullOrBlank;
import static dev.langchain4j.internal.Utils.readBytes;
import static dev.langchain4j.internal.ValidationUtils.ensureGreaterThanZero;
import static dev.langchain4j.internal.ValidationUtils.ensureNotBlank;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresEngine {

  private DataSource dataSource;
  private static final Logger log = LoggerFactory.getLogger(PostgresEngine.class.getName());

  /**
   * Constructor for PostgresEngine
   *
   * @param projectId (Required) project id
   * @param region (Required) cluster region
   * @param cluster (Required) cluster
   * @param instance (Required) instance
   * @param database (Required) database
   * @param user (Optional) database user
   * @param password (Optional) database password
   * @param ipType (Required) type of IP to be used (PUBLIC, PSC)
   * @param iamAccountEmail (Optional) IAM account email
   */
  public PostgresEngine(
      String projectId,
      String region,
      String instance,
      String database,
      String user,
      String password,
      String ipType,
      String iamAccountEmail) {
    Boolean enableIAMAuth = false;

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
      throw new IllegalStateException(
          "Either one of user or password is blank, expected both user and password to be valid"
              + " credentials or empty");
    }
    String instanceName =
        new StringBuilder(ensureNotBlank(projectId, "projectId"))
            .append(":")
            .append(ensureNotBlank(region, "region"))
            .append(":")
            .append(ensureNotBlank(instance, "instance"))
            .toString();
    dataSource = createDataSource(database, user, password, instanceName, ipType, enableIAMAuth);
  }

  private HikariDataSource createDataSource(
      String database,
      String user,
      String password,
      String instanceName,
      String ipType,
      Boolean enableIAMAuth) {
    HikariConfig config = new HikariConfig();
    config.setUsername(ensureNotBlank(user, "user"));
    if (enableIAMAuth) {
      config.addDataSourceProperty("enableIAMAuth", "true");
    } else {
      config.setPassword(ensureNotBlank(password, "password"));
    }
    config.setJdbcUrl(String.format("jdbc:postgresql:///%s", ensureNotBlank(database, "database")));
    config.addDataSourceProperty("socketFactory", "com.google.cloud.postgres.SocketFactory");
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
    return dataSource.getConnection();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private String projectId;
    private String region;
    private String instance;
    private String database;
    private String user;
    private String password;
    private String ipType;
    private String iamAccountEmail;

    public Builder() {}

    /**
     * @param projectId (Required) project id
     */
    public Builder projectId(String projectId) {
      this.projectId = projectId;
      return this;
    }

    /**
     * @param region (Required) cluster region
     */
    public Builder region(String region) {
      this.region = region;
      return this;
    }

    /**
     * @param instance (Required) instance
     */
    public Builder instance(String instance) {
      this.instance = instance;
      return this;
    }

    /**
     * @param database (Required) database
     */
    public Builder database(String database) {
      this.database = database;
      return this;
    }

    /**
     * @param database (Required) database user
     */
    public Builder user(String user) {
      this.user = user;
      return this;
    }

    /**
     * @param database (Required) database password
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

    PostgresEngine build() {
      return new PostgresEngine(
          projectId, region, instance, database, user, password, ipType, iamAccountEmail);
    }
  }

  /**
   * @param embeddingStoreConfig contains the parameters necesary to intialize the Vector table
   */
  public void initVectorStoreTable(EmbeddingStoreConfig embeddingStoreConfig) {

    try (Connection connection = getConnection(); ) {
      Statement statement = connection.createStatement();
      statement.executeUpdate("CREATE EXTENSION IF NOT EXISTS vector");

      if (embeddingStoreConfig.getOverwriteExisting()) {
        statement.executeUpdate(
            String.format(
                "DROP TABLE \"%s\".\"%s\"",
                embeddingStoreConfig.getSchemaName(), embeddingStoreConfig.getTableName()));
      }

      String metadataClause = "";
      if (embeddingStoreConfig.getMetadataColumns() != null
          && !embeddingStoreConfig.getMetadataColumns().isEmpty()) {
        if (!embeddingStoreConfig.getStoreMetadata()) {
          throw new IllegalStateException(
              "storeMetadata option is disabled but metadata was provided");
        }
        metadataClause =
            String.format(
                ", %s",
                embeddingStoreConfig.getMetadataColumns().stream()
                    .map(MetadataColumn::generateColumnString)
                    .collect(Collectors.joining(",")));
      } else if (embeddingStoreConfig.getStoreMetadata()) {
        throw new IllegalStateException(
            "storeMetadata option is enabled but no metadata was provided");
      }

      if (embeddingStoreConfig.getStoreMetadata()) {
        metadataClause +=
            String.format(
                ", %s",
                new MetadataColumn(embeddingStoreConfig.getMetadataJsonColumn(), "JSON", true)
                    .generateColumnString());
      }

      String query =
          String.format(
              "CREATE \"%s\".\"%s\" (\"%s\" UUID PRIMARY KEY, \"%s\" TEXT NOT NULL, \"%s\""
                  + " vector(%d) NOT NULL%s)",
              embeddingStoreConfig.getSchemaName(),
              embeddingStoreConfig.getTableName(),
              embeddingStoreConfig.getIdColumn(),
              embeddingStoreConfig.getContentColumn(),
              embeddingStoreConfig.getEmbeddingColumn(),
              ensureGreaterThanZero(embeddingStoreConfig.getVectorSize(), "vectorSize"),
              metadataClause);
      statement.executeUpdate(query);

    } catch (SQLException ex) {
      throw new RuntimeException(
          String.format(
              "Failed to initialize vector store table: \"%s\".\"%s\"",
              embeddingStoreConfig.getSchemaName(), embeddingStoreConfig.getTableName(), ex));
    }
  }
}
