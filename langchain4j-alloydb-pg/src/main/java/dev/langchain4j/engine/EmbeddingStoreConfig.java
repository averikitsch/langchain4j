package dev.langchain4j.engine;

import java.util.List;

import static dev.langchain4j.internal.ValidationUtils.ensureGreaterThanZero;
import static dev.langchain4j.internal.ValidationUtils.ensureNotBlank;
import dev.langchain4j.engine.MetadataColumn;

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
     */
    private EmbeddingStoreConfig(String tableName, Integer vectorSize, String schemaName, String contentColumn, String embeddingColumn, String idColumn, List<MetadataColumn> metadataColumns, String metadataJsonColumn, Boolean overwriteExisting, Boolean storeMetadata) {
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
    }

    public static Builder builder() {
        return new Builder();
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

        public Builder() {
            this.schemaName = "public";
            this.contentColumn = "content";
            this.embeddingColumn = "embedding";
            this.idColumn = "langchain_id";
            this.metadataJsonColumn = "langchain_metadata";
            this.overwriteExisting = false;
            this.storeMetadata = false;
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

        public EmbeddingStoreConfig build() {
            return new EmbeddingStoreConfig(tableName, vectorSize, schemaName, contentColumn, embeddingColumn, idColumn, metadataColumns, metadataJsonColumn, overwriteExisting, storeMetadata);
        }
    }
}
