package dev.langchain4j.store.alloydb.index;

import java.util.List;

public interface VectorIndex {

    public String getIndexOptions();

    public List<String> getParameterSettings();
    
}