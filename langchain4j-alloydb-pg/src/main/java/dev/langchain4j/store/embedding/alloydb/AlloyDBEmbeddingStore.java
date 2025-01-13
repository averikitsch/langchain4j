package dev.langchain4j.store.embedding.alloydb;

import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import static dev.langchain4j.internal.ValidationUtils.ensureNotBlank;
import static dev.langchain4j.internal.ValidationUtils.ensureNotNull;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingSearchResult;
import dev.langchain4j.store.embedding.EmbeddingStore;

public class AlloyDBEmbeddingStore implements EmbeddingStore<TextSegment> {

    private static final Logger log = Logger.getLogger(AlloyDBEmbeddingStore.class.getName());
    private final AlloyDBEngine engine;
    private final String tableName;
    private String contentColumn;
    private String embeddingColumn;
    private List<String> metadataColumns;
    private List<String> ignoreMetadataColumns;
    private Boolean overwriteExisting;
    // change to QueryOptions class when implemented
    private List<String> queryOptions;

    /**
     * Constructor for AlloyDBEmbeddingStore
     *
     * @param engine The connection object to use
     * @param tableName The name of the table (no default, user must specify)
     * @param contentColumn (Optional, Default: “content”) Column that represent
     * a Document’s page content
     * @param embeddingColumn (Optional, Default: “embedding”) Column for
     * embedding vectors. The embedding is generated from the document value
     * @param metadataColumns (Optional) Column(s) that represent a document’s
     * metadata
     * @param ignoreMetadataColumns (Optional) Column(s) to ignore in
     * pre-existing tables for a document’s
     * @param overwriteExisting (Optional, Default: False) Boolean argument for
     * truncating table before insertion
     * @param queryOptions (Optional) QueryOptions class with vector search
     * parameters
     */
    public AlloyDBEmbeddingStore(AlloyDBEngine engine, String tableName, String contentColumn, String embeddingColumn, List<String> metadataColumns, List<String> ignoreMetadataColumns, Boolean overwriteExisting, List<String> queryOptions) {
        this.engine = ensureNotNull(engine, "engine");
        this.tableName = ensureNotBlank(tableName, "tableName");
        this.contentColumn = contentColumn.isEmptycontentColumn;
        this.embeddingColumn = embeddingColumn;
        this.metadataColumns = metadataColumns;
        this.ignoreMetadataColumns = ignoreMetadataColumns;
        this.overwriteExisting = overwriteExisting;
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
    public String add(Embedding embedding, TextSegment embedded) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<String> addAll(List<Embedding> embeddings) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public EmbeddingSearchResult<TextSegment> search(EmbeddingSearchRequest request) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void removeAll(Collection<String> ids) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public static class Builder {

        private AlloyDBEngine engine;
        private String tableName;
        private String contentColumn;
        private String embeddingColumn;
        private List<String> metadataColumns;
        private List<String> ignoreMetadataColumns;
        private Boolean overwriteExisting;
        // change to QueryOptions class when implemented
        private List<String> queryOptions;

        public Builder() {
            this.contentColumn = "content";
            this.embeddingColumn = "embedding";
            this.overwrite_existing = false;
        }

        public Builder engine(AlloyDBEngine engine) {
            this.engine = engine;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
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

        public Builder metadataColumns(List<String> metadataColumns) {
            this.metadataColumns = metadataColumns;
            return this;
        }

        public Builder ignoreMetadataColumns(List<String> ignoreMetadataColumns) {
            this.ignoreMetadataColumns = ignoreMetadataColumns;
            return this;
        }

        public Builder overwriteExisting(Boolean overwriteExisting) {
            this.overwriteExisting = overwriteExisting;
            return this;
        }

        public Builder queryOptions(List<String> queryOptions) {
            this.queryOptions = queryOptions;
            return this;
        }

        public AlloyDBEngine build() {
            return new AlloyDBEmbeddingStore(engine, tableName, contentColumn, embeddingColumn, metadataColumns, ignoreMetadataColumns, overwriteExisting, queryOptions);
        }
    }

}
