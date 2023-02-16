# Snapshot Streaming

## Building

In the root directory of [tessellation](https://github.com/cngo-github/tessellation) run

```
git checkout v1.9.1
sbt kernel/publishM2 shared/publishM2 keytool/publishM2 sdk/publishM2 dagShared/publishM2
```

In the root directory of this repo run

```
sbt assembly
```

The first step is necessary because tessellation artifacts 
are not yet available in a public repository. 

## Running Snapshot Streaming in Kubernetes

In the root directory of tessellation run

```
skaffold dev --trigger=manual
```

In the root directory of this repo run

```
skaffold dev --trigger=manual
```