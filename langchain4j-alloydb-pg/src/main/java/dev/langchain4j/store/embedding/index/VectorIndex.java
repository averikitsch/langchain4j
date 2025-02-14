package dev.langchain4j.store.embedding.index;

public interface VectorIndex {

    final String DEFAULT_INDEX_NAME_SUFFIX = "langchainvectorindex";

    public String getIndexOptions();

}
