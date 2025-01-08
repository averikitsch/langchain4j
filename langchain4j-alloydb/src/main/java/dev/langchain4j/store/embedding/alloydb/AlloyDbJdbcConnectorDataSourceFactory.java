package dev.langchain4j.store.embedding.alloydb;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import static dev.langchain4j.internal.ValidationUtils.ensureNotBlank;


public class AlloyDbJdbcConnectorDataSourceFactory {
  
    static HikariDataSource createDataSource(String database, String user, String password, String instanceName, String ipType) {
      HikariConfig config = new HikariConfig();
  
      config.setJdbcUrl(String.format("jdbc:postgresql:///%s", ensureNotBlank(database, "database")));
      config.setUsername(ensureNotBlank(user, "user")); // e.g., "postgres"
      config.setPassword(ensureNotBlank(password, "password")); // e.g., "secret-password"
      config.addDataSourceProperty("socketFactory", "com.google.cloud.alloydb.SocketFactory");
      // e.g., "projects/my-project/locations/us-central1/clusters/my-cluster/instances/my-instance"
      config.addDataSourceProperty("alloydbInstanceName", ensureNotBlank(instanceName, "instanceName"));
      config.addDataSourceProperty("alloydbIpType", ensureNotBlank(ipType, "ipType"));

      return new HikariDataSource(config);
    }
  }
