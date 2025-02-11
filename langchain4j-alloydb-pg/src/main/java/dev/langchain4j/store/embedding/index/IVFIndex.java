package dev.langchain4j.store.embedding.index;

public class IVFIndex implements VectorIndex {

    private final Integer listCount;
    private final String quantizer;
    private final DistanceStrategy distanceStrategy;

    public IVFIndex(IndexConfig indexConfig) {
        this.listCount = indexConfig.getListCount();
        this.quantizer = indexConfig.getQuantizer();
        this.distanceStrategy = indexConfig.getDistanceStrategy();
    }

    @Override
    public String getIndexOptions() {
        return String.format("(lists = %s, quantizer = %s)", listCount, quantizer);
    }

    public DistanceStrategy getDistanceStrategy() {
        return distanceStrategy;
    }

}
