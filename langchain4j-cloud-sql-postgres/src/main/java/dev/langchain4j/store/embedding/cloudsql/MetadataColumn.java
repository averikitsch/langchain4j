package dev.langchain4j.engine;

public class MetadataColumn {

    private String name;
    private String type;
    private Boolean nullable;

    public MetadataColumn(String name, String type, Boolean nullable) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
    }

    /**
     * generate the column clause to be used by {@link PostgresEngine}
     *
     * @return column clause for create table
     */
    public String generateColumnString() {
        return String.format("\"%s\" %s %s", name, type, nullable ? "" : "NOT NULL");
    }

    /**
     * the metadata column name
     *
     * @return name string
     */
    public String getName() {
        return name;
    }
}
