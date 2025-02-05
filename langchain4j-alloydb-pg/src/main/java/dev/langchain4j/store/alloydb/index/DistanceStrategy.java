package dev.langchain4j.store.alloydb.index;

public class DistanceStrategy {

    private final String DEFAULT_INDEX_NAME_SUFFIX = "langchainvectorindex";
    private final String operator;
    private final String searchFunction;
    private final String index_function;
    private final String scannIndexFunction;

    private DistanceStrategy(String index_function, String operator, String scannIndexFunction, String searchFunction) {
        this.index_function = index_function;
        this.operator = operator;
        this.scannIndexFunction = scannIndexFunction;
        this.searchFunction = searchFunction;
    }

    public String getOperator() {
        return operator;
    }

    public String getSearchFunction() {
        return searchFunction;
    }

    public String getIndex_function() {
        return index_function;
    }

    public String getScannIndexFunction() {
        return scannIndexFunction;
    }

    public String getDefaultIndexNameSuffix() {
        return DEFAULT_INDEX_NAME_SUFFIX;
    }

    public class DistanceStrategyFactory {

        public static DistanceStrategy getEuclideanDistanceStrategy() {
            return new DistanceStrategy("<->", "l2_distance", "vector_l2_ops", "l2");
        }

        public static DistanceStrategy getCosineDistanceStrategy() {
            return new DistanceStrategy("<=>", "cosine_distance", "vector_cosine_ops", "cosine");
        }

        public static DistanceStrategy getInnerProductDistanceStrategy() {
            return new DistanceStrategy("<#>", "inner_product", "vector_ip_ops", "dot_product");
        }
    }
}
