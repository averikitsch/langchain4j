# AlloyDB for PostgreSQL for LangChain

==================================================

- [Product Documentation](https://cloud.google.com/alloydb)

The **AlloyDB for PostgreSQL for LangChain** package provides a first class experience for connecting to
AlloyDB instances from the LangChain ecosystem while providing the following benefits:

- **Simplified & Secure Connections**: easily and securely create shared connection pools to connect to Google Cloud databases utilizing IAM for authorization and database authentication without needing to manage SSL certificates, configure firewall rules, or enable authorized networks.
- **Improved performance & Simplified management**: use a single-table schema can lead to faster query execution, especially for large collections.
- **Improved metadata handling**: store metadata in columns instead of JSON, resulting in significant performance improvements.
- **Clear separation**: clearly separate table and extension creation, allowing for distinct permissions and streamlined workflows.
- **Better integration with AlloyDB**: built-in methods to take advantage of AlloyDB's advanced indexing and scalability capabilities.


## Quick Start

In order to use this library, you first need to go through the following
steps:

1. [Select or create a Cloud Platform project.](https://console.cloud.google.com/project)
2. [Enable billing for your project.](https://cloud.google.com/billing/docs/how-to/modify-project#enable_billing_for_a_project)
3. [Enable the AlloyDB API.](https://console.cloud.google.com/flows/enableapi?apiid=alloydb.googleapis.com)
4. [Setup Authentication.](https://googleapis.dev/python/google-api-core/latest/auth.html)


### Maven Dependency

```xml
<dependency>
    <groupId>dev.langchain4j</groupId>
    <artificatId>langchain4j-alloydb-pg</artificatId>
    <version>1.0.0-beta2</version>
</dependency>
```

### Supported Java Versions

Java >= 17


## AlloyDBEngine Usage

Instances of AlloyDBEngine can be created by configuring provided Builder, it requires the AlloyDB project-id, region, cluster, instance, database, database user (optional), database password (optional), IAM accont email (optional), ip-type (optional, default: "public"), host (optional), port (optional, default: 5432).
example configuration: 

```java
...
import dev.langchain4j.engine.AlloyDBEngine;
...
    AlloyDBEngine engine = new AlloyDBEngine.Builder()
                .projectId("my-projectId")
                .region("my-region")
                .cluster("my-cluster")
                .instance("my-instance")
                .database("my-database")
                .user("my-user")
                .password("my-password")
                .build();

```
For authentication there will be 3 possible configurations
- provided user and password will be used to loginto the database
- provided IAM account email will be used to login to the database
- if neither the IAM account email or user and password are provided, the Engine will retrieve the account from the authenticated Google credential.

### initVectorStore
AlloyDBEngine provides initVectorStore method to initialize the vector store table, it requires a EmbeddingStoreConfig object that can be configured with its provided Builder it requires table name, vector size, schema name (optional, default: "public"), contentColumn (optional, default: "content"), embeddingColumn(optional, default:"embedding"), idColumn (optional, default:"langchain_id"), metadata columns (optional), metadataJsonColumn (optional, default: "langchain_metadata"), overwriteExisting (optional, default: false), storeMetadata (optional, default: false).
example usage:
```java
...
import dev.langchain4j.engine.EmbeddingStoreConfig;
import dev.langchain4j.engine.MetadataColumn;
...
        List<MetadataColumn> metadataColumns = new ArrayList<>();
        metadataColumns.add(new MetadataColumn("page", "TEXT", true));
        metadataColumns.add(new MetadataColumn("source", "TEXT", false));
        EmbeddingStoreConfig customParams = new EmbeddingStoreConfig.Builder("MY_TABLE_NAME", 768)
                .contentColumn("my_content_column")
                .embeddingColumn("my_embedding_column")
                .metadataColumns(metadataColumns)
                .metadataJsonColumn("my_metadata_json_column")
                .storeMetadata(true)
                .build();
        engine.initVectorStoreTable(customParams);
```

MetadataColumn constructor receives 3 parameters, the name of the column, the column's type and a boolean nullable for using the nullable constraint, supported types are: "text", "char()", "varchar()", "uuid", "integer", "bigint", "real" and "double"


## AlloyDBEmbeddingStore Usage

Use a vector store to store embedded data and perform vector search, instances of AlloyDBEmbeddingStore can be created by configuring provided Builder, it requires an AlloyDBEngine, table name, schema name (optional, default: "public"), content column (optional, default: "content"), embedding column (optional, default: "embedding"), id column (optional, default: "langchain_id"), metadata column names (optional), additional metadata json column (optional, default: "langchain_metadata"), ignored metadata column names (optional), distance strategy (optional, default:DistanceStrategy.COSINE_DISTANCE), query options (optional).


```java
...
    import dev.langchain4j.store.embedding.alloydb.AlloyDBEmbeddingStore;

    AlloyDBEmbeddingStore store = new AlloyDBEmbeddingStore.Builder(engine, TABLE_NAME)
        .build();

    
```
See the full `Vector Store`_ tutorial.

.. _`Vector Store`: https://github.com/googleapis/langchain-google-alloydb-pg-python/tree/main/docs/vector_store.ipynb

Document Loader Usage
~~~~~~~~~~~~~~~~~~~~~

Use a document loader to load data as LangChain ``Document``\ s.

.. code-block:: python

   from langchain_google_alloydb_pg import AlloyDBEngine, AlloyDBLoader


   engine = AlloyDBEngine.from_instance("project-id", "region", "my-cluster", "my-instance", "my-database")
   loader = AlloyDBLoader.create_sync(
       engine,
       table_name="my-table-name"
   )
   docs = loader.lazy_load()

See the full `Document Loader`_ tutorial.

.. _`Document Loader`: https://github.com/googleapis/langchain-google-alloydb-pg-python/tree/main/docs/document_loader.ipynb

Chat Message History Usage
~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``ChatMessageHistory`` to store messages and provide conversation
history to LLMs.

.. code:: python

   from langchain_google_alloydb_pg import AlloyDBChatMessageHistory, AlloyDBEngine


   engine = AlloyDBEngine.from_instance("project-id", "region", "my-cluster", "my-instance", "my-database")
   history = AlloyDBChatMessageHistory.create_sync(
       engine,
       table_name="my-message-store",
       session_id="my-session-id"
   )

See the full `Chat Message History`_ tutorial.

.. _`Chat Message History`: https://github.com/googleapis/langchain-google-alloydb-pg-python/tree/main/docs/chat_message_history.ipynb

Langgraph Checkpoint Usage
~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``AlloyDBSaver`` to save snapshots of the graph state at a given point in time.

.. code:: python

   from langchain_google_alloydb_pg import AlloyDBSaver, AlloyDBEngine


   engine = AlloyDBEngine.from_instance("project-id", "region", "my-cluster", "my-instance", "my-database")
   checkpoint = AlloyDBSaver.create_sync(engine)

See the full `Checkpoint`_ tutorial.

.. _`Checkpoint`: https://github.com/googleapis/langchain-google-alloydb-pg-python/tree/main/docs/langgraph_checkpoint.ipynb

Example Usage
-------------

Code examples can be found in the `samples/`_ folder.

.. _samples/: https://github.com/googleapis/langchain-google-alloydb-pg-python/tree/main/samples

Converting between Sync & Async Usage
-------------------------------------

Async functionality improves the speed and efficiency of database connections through concurrency,
which is key for providing enterprise quality performance and scaling in GenAI applications. This
package uses a native async Postgres driver, `asyncpg`_, to optimize Python's async functionality.

LangChain supports `async programming`_, since LLM based application utilize many I/O-bound operations,
such as making API calls to language models, databases, or other services. All components should provide
both async and sync versions of all methods.

`asyncio`_ is a Python library used for concurrent programming and is used as the foundation for multiple
Python asynchronous frameworks. asyncio uses `async` / `await` syntax to achieve concurrency for
non-blocking I/O-bound tasks using one thread with cooperative multitasking instead of multi-threading.

.. _`async programming`: https://python.langchain.com/docs/concepts/async/
.. _`asyncio`: https://docs.python.org/3/library/asyncio.html
.. _`asyncpg`: https://github.com/MagicStack/asyncpg

Converting Sync to Async
~~~~~~~~~~~~~~~~~~~~~~~~

Update sync methods to `await` async methods

.. code:: python

   engine = await AlloyDBEngine.afrom_instance("project-id", "region", "my-cluster", "my-instance", "my-database")
   await engine.ainit_vectorstore_table(table_name="my-table", vector_size=768)
   vectorstore = await AlloyDBVectorStore.create(
      engine,
      table_name="my-table",
      embedding_service=VertexAIEmbeddings(model_name="textembedding-gecko@003")
   )

Run the code: notebooks
^^^^^^^^^^^^^^^^^^^^^^^

ipython and jupyter notebooks support the use of the `await` keyword without any additional setup

Run the code: FastAPI
^^^^^^^^^^^^^^^^^^^^^

Update routes to use `async def`.

.. code:: python

   @app.get("/invoke/")
   async def invoke(query: str):
      return await retriever.ainvoke(query)


Run the code: Local python file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It is recommend to create a top-level async method definition: `async def` to wrap multiple async methods.
Then use `asyncio.run()` to run the the top-level entrypoint, e.g. "main()"

.. code:: python

   async def main():
      response = await retriever.ainvoke(query)
      print(response)

   asyncio.run(main())


Contributions
-------------

Contributions to this library are always welcome and highly encouraged.

See `CONTRIBUTING`_ and `DEVELOPER`_ for more information how to get started.

Please note that this project is released with a Contributor Code of Conduct. By participating in
this project you agree to abide by its terms. See `Code of Conduct`_ for more
information.

.. _`CONTRIBUTING`: https://github.com/googleapis/langchain-google-alloydb-pg-python/tree/main/CONTRIBUTING.md
.. _`DEVELOPER`: https://github.com/googleapis/langchain-google-alloydb-pg-python/tree/main/DEVELOPER.md
.. _`Code of Conduct`: https://github.com/googleapis/langchain-google-alloydb-pg-python/tree/main/CODE_OF_CONDUCT.md

License
-------

Apache 2.0 - See
`LICENSE <https://github.com/googleapis/langchain-google-alloydb-pg-python/tree/main/LICENSE>`_
for more information.

Disclaimer
----------

This is not an officially supported Google product.



## AlloyDB PostgreSQL Embedding Store for LangChain4j
This module implements `EmbeddingStore` backed by an AlloyDB for PostgreSQL database.
