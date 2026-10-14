# Pipeline: Bronze architecture and logical datasets

A logical dataset is not a physical file. One logical dataset can span many staging files. A large file never becomes many datasets automatically. Many small files with one schema never become many tables.

Three counts are never the same number: the staging dataset count, the physical chunk count and the Bronze table count. Chunking exists only for upload. It never changes dataset boundaries.

Bronze rules:
1. A staging dataset is not automatically a Bronze table.
2. Chunk boundaries carry no schema meaning.
3. One notebook per source reads every chunk in one scan and writes the Bronze tables.
4. Bronze stays 1:1 with the staged shape. Schema merges wait for Silver.
5. Table names are flat: `<catalog>.bronze.<source_prefix>_<dataset>`.

The notebook checks the schema and a non-zero row count. It drops the source's Volume only after a full pass. The four Samples datasets have no Bronze tables.
