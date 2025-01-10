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


    public AlloyDBEmbeddingStore(AlloyDBEngine engine, String tableName) {
        this.engine = ensureNotNull(engine, "engine");
        this.tableName = ensureNotBlank(tableName, "tableName");
        contentColumn = "content";
        embeddingColumn = "embedding";
        overwriteExisting = false;
    }

    public void setContentColumn(String contentColumn) {
        this.contentColumn = contentColumn;
    }

    public void setEmbeddingColumn(String embeddingColumn) {
        this.embeddingColumn = embeddingColumn;
    }

    public void setMetadataColumns(List<String> metadataColumns) {
        this.metadataColumns = metadataColumns;
    }

    public void setIgnoreMetadataColumns(List<String> ignoreMetadataColumns) {
        this.ignoreMetadataColumns = ignoreMetadataColumns;
    }

    public void setOverwriteExisting(Boolean overwriteExisting) {
        this.overwriteExisting = overwriteExisting;
    }

    public void setQueryOptions(List<String> queryOptions) {
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

}
