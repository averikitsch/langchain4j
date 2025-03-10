package dev.langchain4j.engine;

import static dev.langchain4j.internal.ValidationUtils.ensureGreaterThanZero;
import static dev.langchain4j.internal.ValidationUtils.ensureNotBlank;

import java.util.List;

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

    /**
     * create a non-default VectorStore table
     *
     * @param builder, Builder instance containing the necessary data to
     * construct tableName (Required) the table name to create - does not append
     * a suffix or prefix! vectorSize (Required) create a vector column with
     * custom vector size schemaName (Default: "public") The schema name
     * contentColumn (Default: "content") create the content column with custom
     * name embeddingColumn (Default: "embedding") create the embedding column
     * with custom name idColumn (Optional, Default: "langchain_id") Column to
     * store ids. metadataColumns list of SQLAlchemy Columns to create for
     * custom metadata metadataJsonColumn (Default: "langchain_metadata") the
     * column to store extra metadata in overwriteExisting (Default: False)
     * boolean for dropping table before insertion storeMetadata (Default:
     * False) boolean to store extra metadata in metadata column if not
     * described in “metadata” field list
     */
    private EmbeddingStoreConfig(Builder builder) {
        ensureNotBlank(builder.tableName, "tableName");
        ensureGreaterThanZero(builder.vectorSize, "vectorSize");
        this.contentColumn = builder.contentColumn;
        this.embeddingColumn = builder.embeddingColumn;
        this.idColumn = builder.idColumn;
        this.metadataColumns = builder.metadataColumns;
        this.metadataJsonColumn = builder.metadataJsonColumn;
        this.overwriteExisting = builder.overwriteExisting;
        this.schemaName = builder.schemaName;
        this.storeMetadata = builder.storeMetadata;
        this.tableName = builder.tableName;
        this.vectorSize = builder.vectorSize;
    }

    public String getTableName() {
        return tableName;
    }

    public Integer getVectorSize() {
        return vectorSize;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getContentColumn() {
        return contentColumn;
    }

    public String getEmbeddingColumn() {
        return embeddingColumn;
    }

    public String getIdColumn() {
        return idColumn;
    }

    public List<MetadataColumn> getMetadataColumns() {
        return metadataColumns;
    }

    public String getMetadataJsonColumn() {
        return metadataJsonColumn;
    }

    public Boolean getOverwriteExisting() {
        return overwriteExisting;
    }

    public Boolean getStoreMetadata() {
        return storeMetadata;
    }

    public static class Builder {

        private final String tableName;
        private final Integer vectorSize;
        private String schemaName = "public";
        private String contentColumn = "content";
        private String embeddingColumn = "embedding";
        private String idColumn = "langchain_id";
        private List<MetadataColumn> metadataColumns;
        private String metadataJsonColumn = "langchain_metadata";
        private Boolean overwriteExisting = false;
        private Boolean storeMetadata = false;

        /**
         * @return builder instance
         * @param tableName (Required) the table name to create - does not
         * append a suffix or prefix!
         * @param vectorSize (Required) create a vector column with custom
         * vector size
         */
        public Builder(String tableName, Integer vectorSize) {
            this.tableName = tableName;
            this.vectorSize = vectorSize;
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

        public EmbeddingStoreConfig build() {
            return new EmbeddingStoreConfig(this);
        }
    }
}
