package dev.langchain4j.store.embedding.index;

public enum DistanceStrategy {
    EUCLIDEAN("<->", "l2_distance", "vector_l2_ops"),
    COSINE_DISTANCE("<=>", "cosine_distance", "vector_cosine_ops"),
    INNER_PRODUCT("<#>", "inner_product", "vector_ip_ops");

    private final String operator;
    private final String searchFunction;
    private final String indexFunction;

    /** Constructor for DistanceStrategy */
    private DistanceStrategy(String operator, String searchFunction, String indexFunction) {
        this.indexFunction = indexFunction;
        this.operator = operator;

        this.searchFunction = searchFunction;
    }

    /**
     * get operator
     *
     * @return DistanceStrategy's operator
     */
    public String getOperator() {
        return operator;
    }

    /**
     * search function
     *
     * @return DistanceStrategy's search function
     */
    public String getSearchFunction() {
        return searchFunction;
    }

    /**
     * get index function
     *
     * @return DistanceStrategy's index function
     */
    public String getIndexFunction() {
        return indexFunction;
    }
}
