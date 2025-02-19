DUCKDB_VERSION=1.2.0 
wget https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/libduckdb-osx-universal.zip \
    && unzip libduckdb-osx-universal.zip -d libduckdb \
    && mv libduckdb/libduckdb.dylib libduckdb/libduckdb.so.dylib \
    && mv libduckdb/libduckdb.so.dylib /usr/local/lib/libduckdb.so.dylib \
    && mv libduckdb/duckdb.* /usr/local/include \
    && rm -rf libduckdb *.zip
