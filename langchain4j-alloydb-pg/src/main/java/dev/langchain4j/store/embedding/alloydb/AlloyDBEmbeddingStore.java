package dev.langchain4j.store.embedding.alloydb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import static java.util.stream.Collectors.toList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import static dev.langchain4j.internal.Utils.isNotNullOrEmpty;
import static dev.langchain4j.internal.Utils.randomUUID;
import dev.langchain4j.engine.AlloyDBEngine;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingSearchResult;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.alloydb.utils.QueryUtils;

public class AlloyDBEmbeddingStore implements EmbeddingStore<TextSegment> {

    private static final Logger log = LoggerFactory.getLogger(AlloyDBEmbeddingStore.class.getName());
    private final AlloyDBEngine engine;
    private final String tableName;
    private String schemaName;
    private String contentColumn;
    private String embeddingColumn;
    private String embeddingIdColumn;
    private List<MetadataColumn> metadataColumns;
    // should ignoreMetadata be boolean?
    private List<String> ignoreMetadataColumns;
    // change to QueryOptions class when implemented
    private List<String> queryOptions;

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
     * @param embeddingIdColumn (Optional, Default: "langchain_id") Column to
     * store ids.
     * @param metadataColumns (Optional) Column(s) that represent a document’s
     * metadata
     * @param ignoreMetadataColumns (Optional) Column(s) to ignore in
     * pre-existing tables for a document’s
     * @param queryOptions (Optional) QueryOptions class with vector search
     * parameters
     */
    public AlloyDBEmbeddingStore(AlloyDBEngine engine, String tableName, String schemaName, String contentColumn, String embeddingColumn, String embeddingIdColumn, List<MetadataColumn> metadataColumns, List<String> ignoreMetadataColumns, List<String> queryOptions) {
        this.engine = engine;
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.contentColumn = contentColumn;
        this.embeddingColumn = embeddingColumn;
        this.embeddingIdColumn = embeddingIdColumn;
        this.metadataColumns = metadataColumns;
        this.ignoreMetadataColumns = ignoreMetadataColumns;
        this.queryOptions = queryOptions;
    }

    @Override
    public String add(Embedding embedding) {
        String id = randomUUID();
        addInternal(id, embedding, null);
        return id;
    }

    @Override
    public void add(String id, Embedding embedding) {
        addInternal(id, embedding, null);
    }

    @Override
    public String add(Embedding embedding, TextSegment embeddedTextSegment) {
        String id = randomUUID();
        addInternal(id, embedding, embeddedTextSegment);
        return id;
    }

    @Override
    public List<String> addAll(List<Embedding> embeddings) {
        List<String> ids = embeddings.stream().map(ignored -> randomUUID()).collect(toList());
        addAllInternal(ids, embeddings, null);
        return ids;
    }

    @Override
    public List<String> addAll(List<Embedding> embeddings, List<TextSegment> embeddedTextSegment) {
        List<String> ids = embeddings.stream().map(ignored -> randomUUID()).collect(toList());
        addAllInternal(ids, embeddings, embeddedTextSegment);
        return ids;
    }

    @Override
    public EmbeddingSearchResult<TextSegment> search(EmbeddingSearchRequest request) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void removeAll(Collection<String> ids) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private void addInternal(String id, Embedding embedding, TextSegment embeddedTextSegment) {
        try (Connection connection = engine.getConnection()) {
            String embeddedText = embeddedTextSegment != null?embeddedTextSegment.text():null;

            Metadata embeddedMetadata = embeddedTextSegment.metadata();

            Set<String> metaNames = (LinkedHashSet) embeddedMetadata.toMap().keySet();

            String metadataColumnNames = String.join(", ", metaNames);
            if(isNotNullOrEmpty(metadataColumnNames)) {
                metadataColumnNames = ", " + metadataColumnNames;
            }
            // column names separated by comma
            String columnNames = String.format("%s, %s, %s%s", embeddingIdColumn, embeddingColumn, contentColumn, metadataColumnNames);

            String placeholders = QueryUtils.getPreparedStatementParameterPlaceholders(columnNames.length()-1);

            // create query
            String query = String.format("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) %s", tableName, columnNames, placeholders, embeddingIdColumn, QueryUtils.getColumnExclusionClause(columnNames));
            // prepared statement, add parameters
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, id);
                preparedStatement.setObject(2, embedding);
                preparedStatement.setString(3, embeddedText);
                for(int i = 3; i <= columnNames.length()-1; i++) {
                    embeddedMetadata.metadata.get(metadataColumnNames.);
                    preparedStatement.setObject(i, id);
                }
                preparedStatement.executeUpdate();
            }
        } catch (SQLException ex) {
            log.error("Exception caught when updating table " + tableName);
            throw new RuntimeException(ex);
        }
    }

    private void addAllInternal(List<String> ids, List<Embedding> embeddings, List<TextSegment> embeddedTextSegments) {
        if (ids.size() != embeddings.size() || embeddings.size() != embeddedTextSegments.size()) {
            throw new IllegalArgumentException("List parameters are different sizes!");
        }
        try (Connection connection = engine.getConnection()) {

            // create query
            String query = String.format("INSERT INTO %S (%s, %s, %s) VALUES (?, ?, ?) ON CONFLICT (embedding_id)"
                    + " DO UPDATE SET %s = excluded.%s, %s = excluded.%s", tableName, embeddingIdColumn, embeddingColumn, contentColumn, embeddingColumn, embeddingColumn, contentColumn, contentColumn);
            // prepared statement, add parameters
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {

                for (int i = 0; i < ids.size(); i++) {
                    preparedStatement.setString(1, ids.get(i));
                    preparedStatement.setObject(2, embeddings.get(i));
                    preparedStatement.setObject(3, embeddedTextSegments.get(i).text());
                    preparedStatement.addBatch();

                }
                preparedStatement.executeBatch();
            }
        } catch (SQLException ex) {
            log.error("Exception caught when updating table " + tableName);
            throw new RuntimeException(ex);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private AlloyDBEngine engine;
        private String tableName;
        private String schemaName;
        private String contentColumn;
        private String embeddingColumn;
        private String embeddingIdColumn;
        private List<MetadataColumn> metadataColumns;
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

        public Builder embeddingIdColumn(String embeddingIdColumn) {
            this.embeddingIdColumn = embeddingIdColumn;
            return this;
        }

        public Builder metadataColumns(List<MetadataColumn> metadataColumns) {
            this.metadataColumns = metadataColumns;
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
            return new AlloyDBEmbeddingStore(engine, tableName, schemaName, contentColumn, embeddingColumn, embeddingIdColumn, metadataColumns, ignoreMetadataColumns, queryOptions);
        }
    }

}
