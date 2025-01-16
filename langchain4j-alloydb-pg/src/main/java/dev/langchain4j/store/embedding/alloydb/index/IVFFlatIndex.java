package dev.langchain4j.store.embedding.alloydb.index;

import java.util.List;

public class IVFFlatIndex implements IndexType {

    private Integer listCount;
    private QueryOptions queryOptions;
    private List<String> partialIndexes;
    private String distanceStrategy;

    public IVFFlatIndex() {
        // TODO Auto-generated method stub
    }

    @Override
    public String generateCreateIndexQuery() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
