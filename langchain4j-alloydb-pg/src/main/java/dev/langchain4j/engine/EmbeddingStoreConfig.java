package dev.langchain4j.engine;

import java.util.ArrayList;
import java.util.List;

import static dev.langchain4j.internal.ValidationUtils.ensureGreaterThanZero;
import static dev.langchain4j.internal.ValidationUtils.ensureNotBlank;

public class EmbeddingStoreConfig {

    private final String tableName;
    private final Integer vectorSize;
    private final String schemaName;
    private final String contentColumn;
    private final String embeddingColumn;
    private final String idColumn;
    private final List<MetadataColumn> metadataColumns;
    private final String metadataJsonColumn;
    private final Boolean overwriteExisting;
    private final Boolean storeMetadata;
    private List<String> ignoreMetadataColumnNames;
    private DistanceStrategy distanceStrategy;
    private Integer k;
    private Integer fetchK;
    private Double lambdaMult;
    // change to QueryOptions class when implemented
    private List<String> queryOptions;
    private AlloyDBEngine engine;

    /**
     * create a non-default VectorStore table
     *
     * @param tableName (Required) the table name to create - does not append a
     * suffix or prefix!
     * @param vectorSize (Required) create a vector column with custom vector
     * size
     * @param schemaName (Default: "public") The schema name
     * @param contentColumn (Default: "content") create the content column with
     * custom name
     * @param embeddingColumn (Default: "embedding") create the embedding column
     * with custom name
     * @param idColumn (Optional, Default: "langchain_id") Column to store ids.
     * @param metadataColumns list of SQLAlchemy Columns to create for custom
     * metadata
     * @param metadataJsonColumn (Default: "langchain_metadata") the column to
     * store extra metadata in
     * @param overwriteExisting (Default: False) boolean for dropping table
     * before insertion
     * @param storeMetadata (Default: False) boolean to store extra metadata in
     * metadata column if not described in “metadata” field list
     * @param ignoreMetadataColumnNames (Optional) Column(s) to ignore in
     * pre-existing tables for a document’s
     * @param distanceStrategy (Defaults: COSINE_DISTANCE) Distance strategy to
     * use for vector similarity search
     * @param k (Defaults: 4) Number of Documents to return from search
     * @param fetchK (Defaults: 20) Number of Documents to fetch to pass to MMR
     * algorithm
     * @param lambdaMult (Defaults: 0.5): Number between 0 and 1 that determines
     * the degree of diversity among the results with 0 corresponding to maximum
     * diversity and 1 to minimum diversity
     * @param queryOptions (Optional) QueryOptions class with vector search
     * parameters
     */
    private EmbeddingStoreConfig(String tableName, Integer vectorSize, String schemaName, String contentColumn, String embeddingColumn,
            String idColumn, List<MetadataColumn> metadataColumns, String metadataJsonColumn, Boolean overwriteExisting, Boolean storeMetadata,
            List<String> ignoreMetadataColumnNames, DistanceStrategy distanceStrategy, Integer k, Integer fetchK, Double lambdaMult, QueryOptions queryOptions) {
        ensureNotBlank(tableName, "tableName");
        ensureGreaterThanZero(vectorSize, "vectorSize");
        this.contentColumn = contentColumn;
        this.embeddingColumn = embeddingColumn;
        this.idColumn = idColumn;
        this.metadataColumns = metadataColumns;
        this.metadataJsonColumn = metadataJsonColumn;
        this.overwriteExisting = overwriteExisting;
        this.schemaName = schemaName;
        this.storeMetadata = storeMetadata;
        this.tableName = tableName;
        this.vectorSize = vectorSize;
        this.ignoreMetadataColumnNames = ignoreMetadataColumnNames;
        this.distanceStrategy = distanceStrategy;
        this.k = k;
        this.fetchK = fetchK;
        this.lambdaMult = lambdaMult;
        this.queryOptions = queryOptions;
    }

    public String getTableName() {
        return this.tableName;
    }

    public Integer getVectorSize() {
        return this.vectorSize;
    }

    public String getSchemaName() {
        return this.schemaName;
    }

    public String getContentColumn() {
        return this.contentColumn;
    }

    public String getEmbeddingColumn() {
        return this.embeddingColumn;
    }

    public String getIdColumn() {
        return this.idColumn;
    }

    public List<MetadataColumn> getMetadataColumns() {
        return this.metadataColumns;
    }

    public String getMetadataJsonColumn() {
        return this.metadataJsonColumn;
    }

    public Boolean getOverwriteExisting() {
        return this.overwriteExisting;
    }

    public Boolean getStoreMetadata() {
        return this.storeMetadata;
    }

    public Boolean isStoreMetadata() {
        return this.storeMetadata;
    }

    public List<String> getIgnoreMetadataColumnNames() {
        return this.ignoreMetadataColumnNames;
    }

    public DistanceStrategy getDistanceStrategy() {
        return this.distanceStrategy;
    }

    public Integer getK() {
        return this.k;
    }

    public Integer getFetchK() {
        return this.fetchK;
    }

    public Double getLambdaMult() {
        return this.lambdaMult;
    }

    public List<String> getQueryOptions() {
        return this.queryOptions;
    }

    public AlloyDBEngine getEngine() {
        return this.engine;
    }

    public static class Builder {

        private String tableName;
        private Integer vectorSize;
        private String schemaName;
        private String contentColumn;
        private String embeddingColumn;
        private String idColumn;
        private List<MetadataColumn> metadataColumns;
        private String metadataJsonColumn;
        private Boolean overwriteExisting;
        private Boolean storeMetadata;
        private List<String> ignoreMetadataColumnNames;
        private DistanceStrategy distanceStrategy;
        private Integer k;
        private Integer fetchK;
        private Double lambdaMult;
        // change to QueryOptions class when implemented
        private List<String> queryOptions;
        private AlloyDBEngine engine;

        public Builder() {
            this.schemaName = "public";
            this.contentColumn = "content";
            this.embeddingColumn = "embedding";
            this.idColumn = "langchain_id";
            this.metadataJsonColumn = "langchain_metadata";
            this.overwriteExisting = false;
            this.storeMetadata = false;
            this.ignoreMetadataColumnNames = new ArrayList();
            this.distanceStrategy = DistanceStrategy.COSINE_DISTANCE;
            this.k = 4;
            this.fetchK = 20;
            this.lambdaMult = 0.5;
        }

        /**
         * @param tableName (Required) the table name to create - does not
         * append a suffix or prefix!
         */
        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        /**
         * @param vectorSize (Required) create a vector column with custom
         * vector size
         */
        public Builder vectorSize(Integer vectorSize) {
            this.vectorSize = vectorSize;
            return this;
        }

        /**
         * @param schemaName (Default: "public") The schema name
         */
        public Builder schemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        /**
         * @param contentColumn (Default: "content") create the content column
         * with custom name
         */
        public Builder contentColumn(String contentColumn) {
            this.contentColumn = contentColumn;
            return this;
        }

        /**
         * @param embeddingColumn (Default: "embedding") create the embedding
         * column with custom name
         */
        public Builder embeddingColumn(String embeddingColumn) {
            this.embeddingColumn = embeddingColumn;
            return this;
        }

        /**
         * @param idColumn (Optional, Default: "langchain_id") Column to store
         * ids.
         */
        public Builder idColumn(String idColumn) {
            this.idColumn = idColumn;
            return this;
        }

        /**
         * @param metadataColumns list of SQLAlchemy Columns to create for
         * custom metadata
         */
        public Builder metadataColumns(List<MetadataColumn> metadataColumns) {
            this.metadataColumns = metadataColumns;
            return this;
        }

        /**
         * @param metadataJsonColumn (Default: "langchain_metadata") the column
         * to store extra metadata in
         */
        public Builder metadataJsonColumn(String metadataJsonColumn) {
            this.metadataJsonColumn = metadataJsonColumn;
            return this;
        }

        /**
         * @param overwriteExisting (Default: False) boolean for dropping table
         * before insertion
         */
        public Builder overwriteExisting(Boolean overwriteExisting) {
            this.overwriteExisting = overwriteExisting;
            return this;
        }

        /**
         * @param storeMetadata (Default: False) boolean to store extra metadata
         * in metadata column if not described in “metadata” field list
         */
        public Builder storeMetadata(Boolean storeMetadata) {
            this.storeMetadata = storeMetadata;
            return this;
        }

        /**
         * @param ignoreMetadataColumnNames (Optional) Column(s) to ignore in
         * pre-existing tables for a document’s
         */
        public Builder ignoreMetadataColumnNames(List<String> ignoreMetadataColumnNames) {
            this.ignoreMetadataColumnNames = ignoreMetadataColumnNames;
            return this;
        }

        /**
         * @param distanceStrategy (Defaults: COSINE_DISTANCE) Distance strategy
         * to use for vector similarity search
         */
        public Builder distanceStrategy(DistanceStrategy distanceStrategy) {
            this.distanceStrategy = distanceStrategy;
            return this;
        }

        /**
         * @param k (Defaults: 4) Number of Documents to return from search
         */
        public Builder k(Integer k) {
            this.k = k;
            return this;
        }

        /**
         * @param fetchK (Defaults: 20) Number of Documents to fetch to pass to
         * MMR algorithm
         */
        public Builder fetchK(Integer fetchK) {
            this.fetchK = fetchK;
            return this;
        }

        /**
         * @param lambdaMult (Defaults: 0.5): Number between 0 and 1 that
         * determines the degree of diversity among the results with 0
         * corresponding to maximum diversity and 1 to minimum diversity
         */
        public Builder lambdaMult(Double lambdaMult) {
            this.lambdaMult = lambdaMult;
            return this;
        }

        /**
         * @param queryOptions (Optional) QueryOptions class with vector search
         * parameters
         */
        public Builder queryOptions(List<String> queryOptions) {
            this.queryOptions = queryOptions;
            return this;
        }

        /**
         * @param engine The connection object to use
         *
         */
        public Builder engine(AlloyDBEngine engine) {
            this.engine = engine;
            return this;
        }

        public EmbeddingStoreConfig build() {
            return new EmbeddingStoreConfig(tableName, vectorSize, schemaName, contentColumn, embeddingColumn,
                    idColumn, metadataColumns, metadataJsonColumn, overwriteExisting, storeMetadata, ignoreMetadataColumnNames,
                    distanceStrategy, k, fetchK, lambdaMult, queryOptions, engine);
        }
    }
}
