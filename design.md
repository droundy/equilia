# Design principles

<details>
<summary><h3>Fully columnar storage</h3><em>
Columns are single large files within a directory structure that defines tables
etc. Most columns will include run length encoding, and the primitive behavior
of a column is iteration from an offset, no random access.
</em>
</summary>

This is the primary feature that makes clickhouse so fast for some kinds of queries.  It
also makes adding a column to a table a pretty easy and efficient process (not that it *needs* to be).
</details>

<details>
<summary><h3>Databases and inserts/mutations are isomorphic</h3><em>
An insert/mutation will be a directory with a set of tables in it to be merged,
just like a database.  The wire protocol (when it exists) will send something
like a zip file of this directory structure.  This will enable a transaction to
be represented as a single insertion since all changes to tables (no plans for
alter) can be represented as insertions.
</em>
</summary>

This is a key feature and bears some discussion.
</details>

<details>
<summary><h3>All table mutations are commutative and associative (for now).</h3><em>
All mutations have a merge result that is independent of the order of insertions
or merges, to ease the consistency of replication.  This has some annoying
implications for deletes, but is huge in terms of the correctness of
replication, since it means that inserts into different replicas need not be
coordinated.
</em>
</summary>

Because mutations are associative and commutative, we can have uncoordinated
writers inserting into replicas without suffering from race conditions, even if
two replicas are disconnected with each other for a long period of time.

This feature is present in clickhouse for some `MergeTree` engines, but not for others, which
has annoying implications in terms of ease of use.
</details>

<details>
<summary><h3>The client is thick.</h3><em>

Network protocol will assume that the client does a fair amount of work, so the
client will have to run our rust library.  This will increase the efficiency of
the server and reduce network bandwidth at the cost of client CPU time.  Joins
will be done on the client if at all (See client library).  If SQL is ever
supported, it will be parsed in the client.

By the same token, we can have both sharding and replication done on the client
rather than the server.  For cases where the client-server network connection is
much slower than the server-server connection, we could introduce server-side
replication, but let’s start by assuming that the client is well connected.
This allows us to simplify the server.  We probably will eventually want server-side
replication (and maybe even sharding) in order to better cope with clients that
crash before finishing insertions into all replicas.  But we can postpone this,
and the replication protocol can be essentially identical (if not actually identical)
to the insertion protocol.
</em>
</summary>

Note that doing replication on the client is made possible because insertions are
commutative, which means that there is not a race condition between multiple clients
that might be inserting into multiple replicas in different orders.
</details>

<details>
<summary><h3>Split logical columns (minor feature)</h3><em>

Transformations of column types, e.g. dates and splitting a logical column into
two that are at different places in the order e.g. partition by month.  So most
significant bits earlier in the sort, for instance.
</em>
</summary>
</details>

<details>
<summary><h3>Aggregating columns</h3><em>

All columns are either in the primary order sequence, or are "aggregating
columns" which have specific behavior when identical primary keys are
encountered, such as summing, computing max or min, or "replacing".
</em></summary>

These aggregating columns will enable the functionality present in the many
different clickhouse `MergeTree` engines plus more (e.g. tracking the first
and last values of a column).
</details>

## Functions and structs to create

1. `enum` for column types
    a. `Bool` stores only run length
    b. `u64` with Max and minimum number of bytes
    c. bytes with prefix and run length
    d. `Deletable<T>` kind of like `Option<T>`?
2. `struct` for table schema
2. `struct` for db schema
2. Individual column formats
    a. Writes file from iterator
    b. Iterates through file from correct offset and row number
3. `enum` for value
4. `struct` for row
5. Nested iterator over columns.
    - Iterates first over first column and for each value iterates offer next column and so on
    - Struct to sort rows and produce a nested iterator
    - Write columns given a nested iterator
    - File system layout looks like `ChunkId/TableName/Column#-ColumnName-ColumnType-MergeRule`

6. Merge rules
    a. Sorted columns must come first in the order of columns.
        I think not… Deletable columns are sortable columns that make have a Deleted value. A Deleted value indicates that all rows matching the prior columns have been deleted.  Any column following a Deletable column must also be Deletable.  Deletable columns may also have the values DeleteStart or DeleteEnd.  Those must always follow each other and indicate that a whole range of rows have been deleted.
    b. Aggregation rules.  These columns will have one value per unique combination of sorted row values
    c. DeleteOneRow like clickhouse sign?
    d. Max
    e. Min
    f. Sum
    g. WithMinMax(column) takes the corresponding value to the value in a Max or Min column.  This enables mutable rows, among other features.
    h. IsDeleted is a bool column than unlike other aggregation columns is ordered first.
    i. TTL causes data to be deleted at the specified time.
7. Merge function
    - Takes two (or more?) chunk nested iterators and produces a new chunk iterator

8. Benchmarks
    1. Obtain data from text corpus
    2. Benchmark creating table through insertions
    3. Benchmark stone simple queries
    4. Single column query type, recursive.

9. Multi column query type
10. Indexer which reads the columns and indexes "granules" with max and min values, where the indexes are held in memory, since column types only support sequential iteration.
