package dev.langchain4j.store.embedding.alloydb;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.engine.AlloyDBEngine;
import static dev.langchain4j.internal.Utils.isNotNullOrBlank;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingSearchResult;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.filter.AlloyDBFilterMapper;
import dev.langchain4j.store.embedding.index.query.QueryOptions;

public class AlloyDBEmbeddingStore implements EmbeddingStore<TextSegment> {

    private static final Logger log = LoggerFactory.getLogger(AlloyDBEmbeddingStore.class.getName());
    private final AlloyDBEngine engine;
    private final String tableName;
    private String schemaName;
    private String contentColumn;
    private String embeddingColumn;
    private String idColumn;
    private List<String> metadataColumns;
    private String metadataJsonColumn;
    private List<String> ignoreMetadataColumns;
    private QueryOptions queryOptions;

    /**
     * Constructor for AlloyDBEmbeddingStore
     *
     * @param engine The connection object to use
     * @param tableName The name of the table (no default, user must specify)
     * @param schemaName (Optional, Default: "public") The schema name
     * @param contentColumn (Optional, Default: “content”) Column that represent
     * a Document’s page content
     * @param embeddingColumn (Optional, Default: “embedding”) Column for
     * embedding vectors. The embedding is generated from the document value
     * @param idColumn (Optional, Default: "langchain_id") Column to store ids.
     * @param metadataColumns (Optional) Column(s) that represent a document’s
     * metadata
     * @param metadataJsonColumn (Optional, Default: "langchain_metadata") The
     * column to store extra metadata in JSON format.
     * @param ignoreMetadataColumns (Optional) Column(s) to ignore in
     * pre-existing tables for a document’s
     * @param queryOptions (Optional) QueryOptions class with vector search
     * parameters
     */
    public AlloyDBEmbeddingStore(AlloyDBEngine engine, String tableName, String schemaName, String contentColumn, String embeddingColumn, String idColumn, List<String> metadataColumns, String metadataJsonColumn, List<String> ignoreMetadataColumns, QueryOptions queryOptions) {
        this.engine = engine;
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.contentColumn = contentColumn;
        this.embeddingColumn = embeddingColumn;
        this.idColumn = idColumn;
        this.metadataColumns = metadataColumns;
        this.metadataJsonColumn = metadataJsonColumn;
        this.ignoreMetadataColumns = ignoreMetadataColumns;
        this.queryOptions = queryOptions;
    }

    @Override
    public String add(Embedding embedding) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void add(String id, Embedding embedding) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<String> addAll(List<Embedding> embeddings) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String add(Embedding embedding, TextSegment embedded) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public List<String> addAll(List<Embedding> embeddings, List<TextSegment> embedded) {
        throw new UnsupportedOperationException("Not supported yet.");
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

        String columnNames = columns.stream().collect(Collectors.joining(", "));

        String whereClause = AlloyDBFilterMapper.generateWhereClause(request.filter());

        String vector = Arrays.toString(request.queryEmbedding().vector());

        String query = String.format("SELECT %s, %s(%s, %s) as distance FROM \"%s\".\"%s\" %s ORDER BY %s %s %s LIMIT %d;",
                columnNames, distanceStrategy.getSearchFunction(), embeddingColumn, vector, schemaName, tableName, whereClause,
                embeddingColumn, distanceStrategy.getOperator(), vector, request.maxResults());
        List<EmbeddingMatch<TextSegment>> embeddingMatches = new ArrayList<>();
        try (Connection conn = engine.getConnection()) {

            try (Statement statement = conn.createStatement()) {
                if (queryOptions != null) {
                    for (String option : queryOptions.getParameterSettings()) {
                        statement.executeQuery(String.format("SET LOCAL %s;", option));
                    }
                }
                ResultSet resultSet = statement.executeQuery(query);
                double distance = resultSet.getDouble("distance");
                String embeddingId = resultSet.getString(idColumn);
                Embedding embedding = resultSet.getObject(embeddingColumn, Embedding.class);
                TextSegment embedded = resultSet.getObject(contentColumn, TextSegment.class);

                embeddingMatches.add(new EmbeddingMatch<>(distance, embeddingId, embedding, embedded));
            }

        } catch (SQLException ex) {
            throw new RuntimeException("Exception caught when searching in store table: \"" + schemaName + "\".\"" + tableName + "\"", ex);
        }
        return new EmbeddingSearchResult<>(embeddingMatches);

    }

    @Override
    public void removeAll(Collection<String> ids) {
        if (ids == null || ids.isEmpty()) {
            throw new IllegalArgumentException("ids must not be null or empty");
        }

        String query = String.format("DELETE FROM \"%s\".\"%s\" WHERE %s IN (?)", schemaName, tableName, idColumn);

        try (Connection conn = engine.getConnection()) {
            try (PreparedStatement preparedStatement = conn.prepareStatement(query)) {
                Array array = conn.createArrayOf("uuid", ids.stream().map(UUID::fromString).toArray());
                preparedStatement.setArray(1, array);
                preparedStatement.executeUpdate();
            }
        } catch (SQLException ex) {
            log.error(String.format("Exception caught when inserting into vector store table: \"%s\".\"%s\"",
                    schemaName, tableName), ex);
            throw new RuntimeException(ex);
        }

    }

    public static class Builder {

        private AlloyDBEngine engine;
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

        public Builder engine(AlloyDBEngine engine) {
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

        public AlloyDBEmbeddingStore build() {
            return new AlloyDBEmbeddingStore(engine, tableName, schemaName, contentColumn, embeddingColumn, idColumn, metadataColumns, metadataJsonColumn, ignoreMetadataColumns, queryOptions);
        }
    }

}
