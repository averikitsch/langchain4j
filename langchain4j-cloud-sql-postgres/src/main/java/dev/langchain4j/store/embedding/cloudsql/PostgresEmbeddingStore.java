package dev.langchain4j.store.embedding.cloudsql;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;
import static dev.langchain4j.internal.Utils.getOrDefault;
import static dev.langchain4j.internal.Utils.isNotNullOrBlank;
import static dev.langchain4j.internal.ValidationUtils.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pgvector.PGvector;
import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.engine.PostgresEngine;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingSearchResult;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.filter.CloudsqlFilterMapper;
import dev.langchain4j.store.embedding.index.DistanceStrategy;
import dev.langchain4j.store.embedding.index.query.QueryOptions;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.UUID;
import java.util.stream.Collectors;
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

    private final CloudsqlFilterMapper FILTER_MAPPER = new CloudsqlFilterMapper();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().enable(INDENT_OUTPUT);

    private QueryOptions queryOptions;
    private String metadataJsonColumn;
    private final DistanceStrategy distanceStrategy;

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
    public PostgresEmbeddingStore(Builder builder) {
        this.engine = builder.engine;
        this.tableName = builder.tableName;
        this.schemaName = builder.schemaName;
        this.contentColumn = builder.contentColumn;
        this.embeddingColumn = builder.embeddingColumn;
        this.idColumn = builder.idColumn;
        this.metadataColumns = builder.metadataColumns;
        this.queryOptions = builder.queryOptions;
        this.metadataJsonColumn = builder.metadataJsonColumn;
        this.distanceStrategy = builder.distanceStrategy;
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

        String query = String.format("DELETE FROM \"%s\".\"%s\" WHERE %s IN (?)", schemaName, tableName, idColumn);

        try (Connection conn = engine.getConnection()) {
            try (PreparedStatement preparedStatement = conn.prepareStatement(query)) {
                Array array = conn.createArrayOf(
                        "uuid", ids.stream().map(UUID::fromString).toArray());
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

    @Override
    public EmbeddingSearchResult<TextSegment> search(EmbeddingSearchRequest request) {
        List<String> columns = new ArrayList(metadataColumns);
        columns.add(idColumn);
        columns.add(contentColumn);
        columns.add(embeddingColumn);
        if (isNotNullOrBlank(metadataJsonColumn)) {
            columns.add(metadataJsonColumn);
        }

        String columnNames =
                columns.stream().map(c -> String.format("\"%s\"", c)).collect(Collectors.joining(", "));

        String filterString = FILTER_MAPPER.map(request.filter());

        String whereClause = isNotNullOrBlank(filterString) ? String.format("WHERE %s", filterString) : "";

        String vector =
                String.format("'%s'", Arrays.toString(request.queryEmbedding().vector()));

        String query = String.format(
                "SELECT %s, %s(%s, %s) as distance FROM \"%s\".\"%s\" %s ORDER BY %s %s %s LIMIT %d;",
                columnNames,
                distanceStrategy.getSearchFunction(),
                embeddingColumn,
                vector,
                schemaName,
                tableName,
                whereClause,
                embeddingColumn,
                distanceStrategy.getOperator(),
                vector,
                request.maxResults());

        List<EmbeddingMatch<TextSegment>> embeddingMatches = new ArrayList<>();

        try (Connection conn = engine.getConnection()) {
            try (Statement statement = conn.createStatement()) {
                if (queryOptions != null) {
                    for (String option : queryOptions.getParameterSettings()) {
                        statement.executeQuery(String.format("SET LOCAL %s;", option));
                    }
                }
                ResultSet resultSet = statement.executeQuery(query);
                while (resultSet.next()) {
                    double distance = resultSet.getDouble("distance");
                    String embeddingId = resultSet.getString(idColumn);

                    PGvector pgVector = (PGvector) resultSet.getObject(embeddingColumn);

                    Embedding embedding = Embedding.from(pgVector.toArray());

                    String embeddedText = resultSet.getString(contentColumn);
                    Map<String, Object> metadataMap = new HashMap<>();

                    for (String metaColumn : metadataColumns) {
                        metadataMap.put(metaColumn, resultSet.getObject(metaColumn));
                    }

                    if (isNotNullOrBlank(metadataJsonColumn)) {
                        String metadataJsonString = getOrDefault(resultSet.getString(metadataJsonColumn), "{}");
                        Map<String, Object> metadataJsonMap = OBJECT_MAPPER.readValue(metadataJsonString, Map.class);
                        metadataMap.putAll(metadataJsonMap);
                    }

                    Metadata metadata = Metadata.from(metadataMap);

                    TextSegment embedded = new TextSegment(embeddedText, metadata);

                    embeddingMatches.add(new EmbeddingMatch<>(distance, embeddingId, embedding, embedded));
                }
            } catch (JsonProcessingException ex) {
                throw new RuntimeException("Exception caught when processing JSON metadata", ex);
            }
        } catch (SQLException ex) {
            throw new RuntimeException(
                    "Exception caught when searching in store table: \"" + schemaName + "\".\"" + tableName + "\"", ex);
        }
        return new EmbeddingSearchResult<>(embeddingMatches);
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
        private DistanceStrategy distanceStrategy = DistanceStrategy.COSINE_DISTANCE;
        private QueryOptions queryOptions;

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

        public Builder queryOptions(QueryOptions queryOptions) {
            this.queryOptions = queryOptions;
            return this;
        }

        public Builder distanceStrategy(DistanceStrategy distanceStrategy) {
            this.distanceStrategy = distanceStrategy;
            return this;
        }

        public PostgresEmbeddingStore build() {
            return new PostgresEmbeddingStore(this);
        }
    }
}
