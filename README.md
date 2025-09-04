# Snapshot Streaming

## Building

In the root directory of [tessellation](https://github.com/cngo-github/tessellation) run

```
git checkout v3.3.2
sbt sdk/publishM2
```

In the root directory of this repo run

```
sbt assembly
```

The first step is necessary because tessellation artifacts 
are not yet available in a public repository. 

## Running Snapshot Streaming

In the root directory of this repo run

```
java -cp target/scala-2.13/cl-snapshot-streaming-assembly-4.4.0-<version-id-hash>.jar -Dconfig.file=mainnet-streaming-cfg/application.conf   org.constellation.snapshotstreaming.App
```

(database definition is included in [sql/snapshot.sql](sql/snapshot.sql))