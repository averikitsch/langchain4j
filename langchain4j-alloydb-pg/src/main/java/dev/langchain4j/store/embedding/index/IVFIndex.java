package dev.langchain4j.store.embedding.index;

public class IVFIndex implements VectorIndex {

    private final Integer listCount;
    private final String quantizer;
    private final DistanceStrategy distanceStrategy;

    public IVFIndex(Integer listCount, String quantizer, DistanceStrategy distanceStrategy) {
        this.listCount = (listCount != null) ? listCount : 100;
        this.quantizer = (quantizer != null) ? quantizer : "sq8";
        this.distanceStrategy = distanceStrategy != null ? distanceStrategy : DistanceStrategy.COSINE_DISTANCE;
    }

    @Override
    public String getIndexOptions() {
        return String.format("(lists = %s, quantizer = %s)", listCount, quantizer);
    }

    public DistanceStrategy getDistanceStrategy() {
        return distanceStrategy;
    }

}
