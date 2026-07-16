# Microbench (JMH)

Run these commands from the repository root (`canton`).

## Clean up

```zsh
sbt "microbench/clean"
rm -rf community/microbench/target
rm -rf community/microbench/log
```

Notes:
- `sbt "microbench/clean"` removes SBT-managed build outputs for the `microbench` project.
- `rm -rf` commands are optional hard cleanup for local reruns.

## Run local (quick feedback)

List available benchmarks:

```zsh
sbt "microbench/Jmh/run -l"
```

Run one quick benchmark sample (`LtHash16Benchmark`):

```zsh
sbt "microbench/Jmh/run com.digitalasset.canton.LtHash16Benchmark -wi 1 -i 3 -f1 -t1"
```

## Run performance (more stable results)

Run `LtHash16Benchmark` with stronger settings and export JSON:

```zsh
mkdir -p community/microbench/log
sbt "microbench/Jmh/run com.digitalasset.canton.LtHash16Benchmark -wi 5 -i 10 -f3 -t1 -rf json -rff log/lthash16-performance.json"
```

## Run profiling

GC profiling:

```zsh
mkdir -p community/microbench/log
sbt "microbench/Jmh/run com.digitalasset.canton.LtHash16Benchmark -wi 3 -i 5 -f1 -t1 -prof gc"
```

JFR profiling (requires a JDK with JFR support):

```zsh
mkdir -p community/microbench/log/jfr
sbt "microbench/Jmh/run com.digitalasset.canton.LtHash16Benchmark -wi 2 -i 5 -f1 -t1 -prof jfr:dir=log/jfr"
```

Stack profiler (sampling):

```zsh
sbt "microbench/Jmh/run com.digitalasset.canton.LtHash16Benchmark -wi 2 -i 5 -f1 -t1 -prof stack"
```

## Useful JMH flags

- `-wi`: warmup iterations
- `-i`: measurement iterations
- `-f`: forks
- `-t`: threads
- `-rf`: result format (`json`, `csv`, `text`)
- `-rff`: result output file
- `-l`: list benchmarks
