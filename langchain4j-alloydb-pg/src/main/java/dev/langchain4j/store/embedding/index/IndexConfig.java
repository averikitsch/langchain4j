package dev.langchain4j.store.embedding.index;

public class IndexConfig {

    private final Integer m;
    private final Integer efConstruction;
    private final Integer listCount;
    private final Integer numLeaves;
    private final String quantizer;
    private final DistanceStrategy distanceStrategy;

    public IndexConfig(Integer m, Integer efConstruction, Integer listCount, Integer numLeaves, String quantizer, DistanceStrategy distanceStrategy) {
        this.m = m;
        this.efConstruction = efConstruction;
        this.listCount = listCount;
        this.numLeaves = numLeaves;
        this.quantizer = quantizer;
        this.distanceStrategy = distanceStrategy;
    }

    public Integer getM() {
        return this.m;
    }

    public Integer getEfConstruction() {
        return this.efConstruction;
    }

    public Integer getListCount() {
        return this.listCount;
    }

    public Integer getNumLeaves() {
        return this.numLeaves;
    }

    public String getQuantizer() {
        return this.quantizer;
    }

    public DistanceStrategy getDistanceStrategy() {
        return this.distanceStrategy;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Integer m;
        private Integer efConstruction;
        private Integer listCount;
        private Integer numLeaves;
        private String quantizer;
        private DistanceStrategy distanceStrategy;

        public Builder() {
            m = 16;
            efConstruction = 64;
            listCount = 100;
            numLeaves = 5;
            quantizer = "sq8";
            distanceStrategy = DistanceStrategy.COSINE_DISTANCE;
        }

        /**
         * @param m Default: 16
         */
        public Builder m(Integer m) {
            this.m = m;
            return this;
        }

        /**
         * @param efConstruction Default: 64
         */
        public Builder efConstruction(Integer efConstruction) {
            this.efConstruction = efConstruction;
            return this;
        }

        /**
         * @param listCount Default: 100
         */
        public Builder listCount(Integer listCount) {
            this.listCount = listCount;
            return this;
        }

        /**
         * @param numLeaves Default: 5
         */
        public Builder numLeaves(Integer numLeaves) {
            this.numLeaves = numLeaves;
            return this;
        }

        /**
         * @param quantizer Default: "sq8"
         */
        public Builder quantizer(String quantizer) {
            this.quantizer = quantizer;
            return this;
        }

        /**
         * @param distanceStrategy Default: DistanceStrategy.COSINE_DISTANCE
         */
        public Builder distanceStrategy(DistanceStrategy distanceStrategy) {
            this.distanceStrategy = distanceStrategy;
            return this;
        }

        public IndexConfig build() {
            return new IndexConfig(m, efConstruction, listCount, numLeaves, quantizer, distanceStrategy);
        }
    }
}
