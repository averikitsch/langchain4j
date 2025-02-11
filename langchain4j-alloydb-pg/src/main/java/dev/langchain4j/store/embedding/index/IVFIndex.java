package dev.langchain4j.store.embedding.index;

import java.util.List;

public class IVFIndex implements VectorIndex {

    private final Integer listCount;
    private final String quantizer;
    private final DistanceStrategy distanceStrategy;
    private final List<String> partialIndexes;


    public IVFIndex(IndexConfig indexConfig) {
        this.listCount = indexConfig.getListCount();
        this.quantizer = indexConfig.getQuantizer();
        this.distanceStrategy = indexConfig.getDistanceStrategy();
        this.partialIndexes = indexConfig.getPartialIndexes();
    }

    @Override
    public String getIndexOptions() {
        return String.format("(lists = %s, quantizer = %s)", listCount, quantizer);
    }

    public DistanceStrategy getDistanceStrategy() {
        return distanceStrategy;
    }

    public List<String> getPartialIndexes() {
        return partialIndexes;
    }

}
