package dev.langchain4j.store.embedding.cloudsql;

import static dev.langchain4j.internal.ValidationUtils.*;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.engine.PostgresEngine;
import dev.langchain4j.store.embedding.EmbeddingStore;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresEmbeddingStore implements EmbeddingStore<TextSegment> {

  private static final Logger log = LoggerFactory.getLogger(PostgresEmbeddingStore.class.getName());
  private final PostgresEngine engine;
  private final String tableName;
  private String schemaName;
  private String contentColumn;
  private String embeddingColumn;
  private String idColumn;
  private List<String> metadataColumns;
  private List<String> queryOptions;

  /**
   * Constructor for PostgresEmbeddingStore
   *
   * @param engine The connection object to use
   * @param tableName The name of the table (no default, user must specify)
   * @param schemaName (Optional, Default: "public") The schema name
   * @param contentColumn (Optional, Default: “content”) Column that represent a Document’s page
   *     content
   * @param embeddingColumn (Optional, Default: “embedding”) Column for embedding vectors. The
   *     embedding is generated from the document value
   * @param idColumn (Optional, Default: "langchain_id") Column to store ids.
   * @param metadataColumns (Optional) Column(s) that represent a document’s metadata
   * @param metadataJsonColumn (Optional, Default: "langchain_metadata") The column to store extra
   *     metadata in JSON format.
   * @param ignoreMetadataColumns (Optional) Column(s) to ignore in pre-existing tables for a
   *     document
   * @param queryOptions (Optional) QueryOptions class with vector search parameters
   */
  public PostgresEmbeddingStore(
      PostgresEngine engine,
      String tableName,
      String schemaName,
      String contentColumn,
      String embeddingColumn,
      String idColumn,
      List<String> metadataColumns,
      String metadataJsonColumn,
      List<String> ignoreMetadataColumns,
      List<String> queryOptions) {
    this.engine = engine;
    this.tableName = tableName;
    this.schemaName = schemaName;
    this.contentColumn = contentColumn;
    this.embeddingColumn = embeddingColumn;
    this.idColumn = idColumn;
    this.metadataColumns = metadataColumns;
    this.queryOptions = queryOptions;
  }

  /**
   * Adds multiple embeddings to the store.
   *
   * @param embeddings a list of embeddings to be added to the store.
   * @param embedded a list of original contents that were embedded.
   */
  @Override
  public List<String> addAll(List<Embedding> embeddings, List<TextSegment> embedded) {
    List<String> ids = new ArrayList<>();
    //   to be implemented
    return ids;
  }

  /**
   * Adds multiple embeddings to the store.
   *
   * @param embeddings a list of embeddings to be added to the store.
   */
  @Override
  public List<String> addAll(List<Embedding> embeddings) {
    // to be implemented
    List<String> ids = new ArrayList<String>();
    return ids;
  }

  /**
   * Adds a given embedding to the store.
   *
   * @param embedding the embedding to be added to the store.
   */
  @Override
  public String add(Embedding embedding) {
    // to be implemented
    return "";
  }

  /**
   * Adds a given embedding to the store.
   *
   * @param embedding the embedding to be added to the store.
   * @param textSegment original content that was embedded.
   */
  @Override
  public String add(Embedding embedding, TextSegment textSegment) {
    // to be implemented
    return "";
  }

  /**
   * Adds a given embedding to the store.
   *
   * @param embedding The embedding to be added to the store.
   */
  @Override
  public void add(String id, Embedding embedding) {
    // to be implemented
  }

  @Override
  public void removeAll(Collection<String> ids) {
    if (ids == null || ids.isEmpty()) {
      throw new IllegalArgumentException("ids must not be null or empty");
    }

    String query =
        String.format("DELETE FROM \"%s\".\"%s\" WHERE %s IN (?)", schemaName, tableName, idColumn);

    try (Connection conn = engine.getConnection()) {
      try (PreparedStatement preparedStatement = conn.prepareStatement(query)) {
        Array array = conn.createArrayOf("uuid", ids.stream().map(UUID::fromString).toArray());
        preparedStatement.setArray(1, array);
        preparedStatement.executeUpdate();
      }
    } catch (SQLException ex) {
      log.error(
          String.format(
              "Exception caught when inserting into vector store table: \"%s\".\"%s\"",
              schemaName, tableName),
          ex);
      throw new RuntimeException(ex);
    }
  }

  public static class Builder {

    private PostgresEngine engine;
    private String tableName;
    private String schemaName;
    private String contentColumn;
    private String embeddingColumn;
    private String idColumn;
    private List<String> metadataColumns;
    private String metadataJsonColumn;
    private List<String> ignoreMetadataColumns;
    // change to QueryOptions class when implemented
    private List<String> queryOptions;

    public Builder() {
      this.contentColumn = "content";
      this.embeddingColumn = "embedding";
    }

    public Builder engine(PostgresEngine engine) {
      this.engine = engine;
      return this;
    }

    public Builder tableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder schemaName(String schemaName) {
      this.schemaName = schemaName;
      return this;
    }

    public Builder contentColumn(String contentColumn) {
      this.contentColumn = contentColumn;
      return this;
    }

    public Builder embeddingColumn(String embeddingColumn) {
      this.embeddingColumn = embeddingColumn;
      return this;
    }

    public Builder idColumn(String idColumn) {
      this.idColumn = idColumn;
      return this;
    }

    public Builder metadataColumns(List<String> metadataColumns) {
      this.metadataColumns = metadataColumns;
      return this;
    }

    public Builder metadataJsonColumn(String metadataJsonColumn) {
      this.metadataJsonColumn = metadataJsonColumn;
      return this;
    }

    public Builder ignoreMetadataColumns(List<String> ignoreMetadataColumns) {
      this.ignoreMetadataColumns = ignoreMetadataColumns;
      return this;
    }

    public Builder queryOptions(List<String> queryOptions) {
      this.queryOptions = queryOptions;
      return this;
    }

    public PostgresEmbeddingStore build() {
      return new PostgresEmbeddingStore(
          engine,
          tableName,
          schemaName,
          contentColumn,
          embeddingColumn,
          idColumn,
          metadataColumns,
          metadataJsonColumn,
          ignoreMetadataColumns,
          queryOptions);
    }
  }
}
