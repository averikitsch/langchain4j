package dev.langchain4j.store.embedding.alloydb.index;

import java.util.List;

import dev.langchain4j.store.embedding.alloydb.QueryOptions;

public class IVFFlatIndex implements IndexType {

    private Integer listCount;
    private QueryOptions queryOptions;
    private List<String> partialIndexes;
    private String distanceStrategy;


    public IVFFlatIndex(Integer listCount, QueryOptions queryOptions, List<String> partialIndexes, String distanceStrategy) {
        this.listCount = listCount;
        this.queryOptions = queryOptions;
        this.partialIndexes = partialIndexes;
        this.distanceStrategy = distanceStrategy;
    }
    

    @Override
    public String generateCreateIndexQuery() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
