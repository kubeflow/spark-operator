# sparkctl

`sparkctl` is a command-line tool of the Spark Operator for creating, listing, checking status of, getting logs of, 
and deleting `SparkApplication`s. It can also do port forwarding from a local port to the Spark web UI port for 
accessing the Spark web UI on the driver. Each function is implemented as a sub-command of `sparkctl`.

To build `sparkctl`, run the following command from within `sparkctl/`:

```bash
go build -o sparkctl
```

## Flags

The following global flags are available for all the sub commands:
* `--namespace`: the Kubernetes namespace of the `SparkApplication`(s).
* `--kubeconfig`: the path to the file storing configuration for accessing the Kubernetes API server.

## Available Commands

### Create

`create` is a sub command of `sparkctl` for parsing and creating a `SparkApplication` object in namespace specified by 
`--namespace` the from a given YAML file. It parses the YAML file, and sends the parsed `SparkApplication` object 
parsed to the Kubernetes API server.

Usage:
```bash
$ sparkctl create <path to YAML file>
```

The `create` command also supports shipping local Hadoop configuration files into the driver and executor pods. 
Specifically, it detects local Hadoop configuration files located at the path specified by the 
environment variable `HADOOP_CONF_DIR`, create a Kubernetes `ConfigMap` from the files, and adds the `ConfigMap` to
the `SparkApplication` object so it gets mounted into the driver and executor pods by the Spark Operator. The 
environment variable `HADOOP_CONF_DIR` is also set in the driver and executor containers.    

It is planned to add support for staging local application dependencies as part of the `create` command in the future.

### List

`list` is a sub command of `sparkctl` for listing `SparkApplication` objects in the namespace specified by 
`--namespace`.

Usage:
```bash
$ sparkctl list
```

### Status

`status` is a sub command of `sparkctl` for checking and printing the status of a `SparkApplication` in the namespace 
specified by `--namespace`.

Usage:
```bash
$ sparkctl status <SparkApplication name>
```

### Log

`log` is a sub command of `sparkctl` for fetching the logs of the driver pod of `SparkApplication` with the given name
in the namespace specified by `--namespace`. Support for fetching executor logs will be added in future.

Usage:
```bash
$ sparkctl log <SparkApplication name>
```

### Delete

`status` is a sub command of `sparkctl` for delete `SparkApplication` with the given name in the namespace 
specified by `--namespace`.

Usage:
```bash
$ sparkctl delete <SparkApplication name>
```

### Forward

`forward` is a sub command of `sparkctl` for doing port forwarding from a local port to the Spark web UI port on the 
driver. It allows the Spark web UI served in the driver pod to be accessed locally. By default, it forwards from local
port `4040` to remote port `4040`, which is the default Spark web UI port. Users can specify different local port
and remote port using the flags `--local-port` and `--remote-port`, respectively. 

Usage:
```bash
$ sparkctl forward <SparkApplication name> [--local-port <local port>] [--remote-port <remote port>]
```

Once port forwarding starts, users can open `127.0.0.1:<local port>` or `localhost:<local port>` in a browser to access
the Spark web UI. Forwarding continues until it is interrupted or the driver pod terminates.