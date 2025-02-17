package dev.langchain4j.store.embedding.index;

import java.util.ArrayList;
import java.util.List;

public class IVFIndex implements BaseIndex {

    private final String indexType;
    private final Integer listCount;
    private final String quantizer;
    private final DistanceStrategy distanceStrategy;
    private final List<String> partialIndexes;

    public IVFIndex(Builder builder) {
        this.indexType = builder.getIndexType();
        this.listCount = builder.getListCount();
        this.quantizer = builder.getQuantizer();
        this.distanceStrategy = builder.getDistanceStrategy();
        this.partialIndexes = builder.getPartialIndexes();
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

    public String getIndexType() {
        return indexType;
    }

    public class Builder {

        private String indexType;
        private Integer listCount;
        private String quantizer;
        private DistanceStrategy distanceStrategy;
        private List<String> partialIndexes;

        public Builder() {
            this.indexType = DEFAULT_INDEX_NAME_SUFFIX;
            listCount = 100;
            quantizer = "sq8";
            distanceStrategy = DistanceStrategy.COSINE_DISTANCE;
            this.partialIndexes = new ArrayList<>();
        }

        public String getIndexType() {
            return indexType;
        }

        public Builder indexType(String indexType) {
            this.indexType = indexType;
            return this;
        }

        public Integer getListCount() {
            return listCount;
        }

        public Builder listCount(Integer listCount) {
            this.listCount = listCount;
            return this;
        }

        public String getQuantizer() {
            return quantizer;
        }

        public Builder quantizer(String quantizer) {
            this.quantizer = quantizer;
            return this;
        }

        public DistanceStrategy getDistanceStrategy() {
            return distanceStrategy;
        }

        public Builder distanceStrategy(DistanceStrategy distanceStrategy) {
            this.distanceStrategy = distanceStrategy;
            return this;
        }

        public List<String> getPartialIndexes() {
            return partialIndexes;
        }

        public Builder partialIndexes(List<String> partialIndexes) {
            this.partialIndexes = partialIndexes;
            return this;
        }

        public IVFIndex build() {
            return new IVFIndex(this);
        }

    }

}
