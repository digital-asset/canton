# Interpreter JMH Benchmarking Tests

## Bench JMH Test

```shell
sbt "daml-lf-interpreter-bench/Jmh/run .*\.Bench \
    [-p b=$B]"
```

## StructProjBench JMH Test

```shell
sbt "daml-lf-interpreter-bench/Jmh/run .*StructProjBench \
    [-p m=$M]                                            \
    [-p n=$N]                                            \
    [-p majorLfVersion=$MAJOR_LF_VERSION]"
```

## SpeedyCompilationBench JMH Test

Without a `darPath` parameter, we default to using `model-tests-3.1.0.dar`

```shell
sbt "daml-lf-interpreter-bench/Jmh/run .*SpeedyCompilationBench \
    [-p darPath=$DAR_FILE]"
```
