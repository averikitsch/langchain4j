package dev.langchain4j.config;

import java.util.ArrayList;
import java.util.List;

import dev.langchain4j.engine.MetadataColumn;
import static dev.langchain4j.internal.ValidationUtils.ensureNotBlank;

public class DocumentTableConfig {

    private final String tableName;
    private final String schemaName;
    private final String contentColumn;
    private final List<MetadataColumn> metadataColumns;
    private final String metadataJsonColumn;
    private final boolean storeMetadata;

    public DocumentTableConfig(String contentColumn, List<MetadataColumn> metadataColumns, String metadataJsonColumn, String schemaName, boolean storeMetadata, String tableName) {
        ensureNotBlank(tableName, "tableName");
        this.contentColumn = contentColumn;
        this.metadataColumns = metadataColumns;
        this.metadataJsonColumn = metadataJsonColumn;
        this.schemaName = schemaName;
        this.storeMetadata = storeMetadata;
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getContentColumn() {
        return contentColumn;
    }

    public List<MetadataColumn> getMetadataColumns() {
        return metadataColumns;
    }

    public String getMetadataJsonColumn() {
        return metadataJsonColumn;
    }

    public boolean isStoreMetadata() {
        return storeMetadata;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String tableName;
        private String schemaName;
        private String contentColumn;
        private List<MetadataColumn> metadataColumns = new ArrayList<>();
        private String metadataJsonColumn;
        private boolean storeMetadata;

        public Builder() {
            this.schemaName = "public";
            this.contentColumn = "page_content";
            this.metadataJsonColumn = "langchain_metadata";
            this.storeMetadata = true;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder schemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public Builder contentColumn(String contentColumn) {
            this.contentColumn = contentColumn;
            return this;
        }

        public Builder metadataColumns(List<MetadataColumn> metadataColumns) {
            this.metadataColumns = metadataColumns;
            return this;
        }

        public Builder addMetadataColumns(MetadataColumn metadataColumns) {
            this.metadataColumns.add(metadataColumns);
            return this;
        }

        public Builder metadataJsonColumn(String metadataJsonColumn) {
            this.metadataJsonColumn = metadataJsonColumn;
            return this;
        }

        public Builder storeMetadata(boolean storeMetadata) {
            this.storeMetadata = storeMetadata;
            return this;
        }

        public DocumentTableConfig build() {
            return new DocumentTableConfig(contentColumn, metadataColumns, metadataJsonColumn, schemaName, storeMetadata, tableName);
        }
    }
}
