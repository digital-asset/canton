## Pruning a sparse Key-Value journal

#### Related Github issue
[[https://github.com/DACH-NY/canton/issues/32045|Here]]

#### Detailed design doc
[[https://docs.google.com/document/d/19L1NmLbn3iIAivzDXaIlqAJnYlDLB7Bhwj4ACVO8Ilw/edit?tab=t.0#heading=h.j1o9vy5fqmrz|Here]]

The following scripts were executed 3 times under the root folder (postgres version 17):
```shell
./run_benchmark.sh -c 1000 -k 10000 -t uuid
./run_benchmark.sh -c 1000 -k 10000 -t tuple
./run_benchmark.sh -c 1000 -k 10000 -t numeric
./run_benchmark.sh -c 1000 -k 10000 -t int16
```
And for version 14 postgres (only once):
```shell
./run_benchmark.sh -c 1000 -k 10000 -t uuid -v 14
./run_benchmark.sh -c 1000 -k 10000 -t tuple -v 14
./run_benchmark.sh -c 1000 -k 10000 -t numeric -v 14
./run_benchmark.sh -c 1000 -k 10000 -t int16 -v 14
```

##### Nomenclature
 - uuid $\to$ A 16-byte `UUID`. The upper 8 bytes represent the ts (timestamp), and the lower 8 bytes represent the tie_breaker.
 - tuple $\to$ A (ts, tie_breaker) pair using two `bigint` columns.
 - numeric $\to$ `NUMERIC(39,0)`. A variable-length type accommodating both values. It is not storage-friendly and was primarily used for experimentation.
 - int16 $\to$ Uses the `uint128` PostgreSQL extension.

> Note: Development for int16 and numeric was abandoned. Neither showed a significant performance advantage, and int16 required a custom Dockerfile to support the external extension.
> Initially, we explored these types because the SQL MAX function does not perform optimally on row tuples (especially if they are part of the index). However, after discovering a way to optimize the queries without relying on MAX for tuples, we returned to the uuid and tuple implementations. Consequently, only the uuid and tuple solutions are up-to-date with the latest query optimizations.

##### Conclusion
Based on the SQL EXPLAIN plans, the tuple (ts, tie_breaker) approach provides the most performant solution.