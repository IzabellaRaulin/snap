Tasks
=====

A task describes the how, what, and when to do for a Snap job.

_Skip to the TL;DR example [here](#tldr)_.
 

## Task States

A task can be in the following states:
- **running:** a running task
- **stopped:** a task that is not running
- **disabled:** a task in a state not allowed to start. This happens when the task produces consecutive errors. A disabled task must be re-enabled before it can be started again. 
- **ended:** a task for which the schedule is ended. It happens for schedule with defined _stop_timestamp_ or with specified the _count_ of runs. An ended task is resumable if the schedule is still valid.

![statediagram](https://cloud.githubusercontent.com/assets/11335874/23774722/62526aaa-0525-11e7-9ce8-894a8e2cbdf1.png)

	    How To				                        |  Command
    ----------------------------------------|------------------------
      Create task                           |  snaptel task create _[command options] [arguments...]_ <br/>  Find more details [here](https://github.com/intelsdi-x/snap/blob/master/docs/SNAPTEL.md#task)
      List					                        |  snaptel task list
      Start task                     		    |  snaptel task start _\<task_id>_
      Stop task                        		  |  snaptel task stop _\<task_id>_
      Remove task                       		|  snaptel task remove _\<task_id>_
      Export task                       		|  snaptel task export _\<task_id>_
      Watch task                        		|  snaptel task watch _\<task_id>_
      Enable task                       		|  snaptel task enable _\<task_id>_


## Task Manifest

A task is described in a task _manifest_, which can be either JSON or YAML<sup>1</sup>. The manifest is divided into two parts: Header and Workflow.

### The Header

```yaml
---
  version: 1
  schedule:
    type: "simple"
    interval: "1s"
  max-failures: 10
```

#### Version
The header contains a version, used to differentiate between versions of the task manifest parser.  Right now, there is only one version: `1`.

#### Schedule

The schedule describes the schedule type and interval for running the task. At the time of this writing, Snap has three schedules:    

- **simple schedule** 
    - a simple "run forever" schedule, which is what we see above as `"simple"`: 
```json
        "version": 1,
        "schedule": {
            "type": "simple",
            "interval": "1s"
        },
        "max-failures": 10,
```

  - or simple "run X times" schedule:
        
```json
        "version": 1,
        "schedule": {
            "type": "simple",
            "interval": "1s",
            "count": 1
        },
        "max-failures": 1,
```
Set `count` to 1 to define _single run task_.   
You can expect that your task will finish in time equals to _count_ multiplied by _interval_.
    
    
- **window schedule** which adds a start and/or stop time for the task. The time must be given as a quoted string in [RFC 3339](https://www.ietf.org/rfc/rfc3339.txt) format, for example with specific time zone offset:
  - a regular window with determined start and stop time
```json
    "version": 1,
    "schedule": {
        "type": "windowed",
        "interval": "1s",
        "start_timestamp": "2016-10-27T16:00:00+01:00",
        "stop_timestamp": "2016-10-28T16:30:00+01:00"
    },
    "max-failures": 10,
```

  - start schedule on _start_timestamp_ and "run forever"
    (a window with determined only stop time)
```json
    "version": 1,
    "schedule": {
        "type": "windowed",
        "interval": "1s",
        "start_timestamp": "2016-10-27T16:00:00+01:00"
    },
    "max-failures": 10,
```

  - start schedule immediately and finish on _stop time_
   (a window with determined only start time)
```json
    "version": 1,
    "schedule": {
        "type": "windowed",
        "interval": "1s",
        "stop_timestamp": "2016-10-28T16:30:00+01:00"
    },
    "max-failures": 10,
```
    
  - start schedule on _start time_ and run "X times"
    (a window with determined the start time and the count)
```json
    "version": 1,
    "schedule": {
        "type": "windowed",
        "interval": "1s",
        "start_timestamp": "2016-10-27T16:00:00+01:00",
        "count": 1,
    },
    "max-failures": 1,
```

  **Notice**: Specyfing both the window _stop time_ and _count_ is not allowed. In such case, you receive a warning that the value of _count_ will be ignored. 
    
- **cron schedule** which supports cron-like entries in ```interval``` field, like in this example (workflow will fire every hour on the half hour):
```json
    "version": 1,
    "schedule": {
        "type": "cron",
        "interval" : "0 30 * * * *"
    },
    "max-failures": 10,
```
More on cron expressions can be found here: https://godoc.org/github.com/robfig/cron

- **others custom schedule** 
Snap is designed in a way where custom schedulers can easily be dropped in. If a custom schedule is used, it may require more key/value pairs in the schedule section of the manifest.

#### Max-Failures

By default, Snap will disable a task if there are 10 consecutive errors from any plugins within the workflow.  The configuration
can be changed by specifying the number of failures value in the task header.  If the `max-failures` value is -1, Snap will
not disable a task with consecutive failure. Instead, Snap will sleep for 1 second for every 10 consecutive failures
and retry again.

If you intend to run tasks with `max-failures: -1`, please also configure `max_plugin_restarts: -1` in [snap daemon control configuration section](SNAPTELD_CONFIGURATION.md).

For more on tasks, visit [`SNAPTEL.md`](SNAPTEL.md).

### The Workflow

```yaml
---
  collect:
    metrics:
      /intel/mock/foo: {}
      /intel/mock/bar: {}
      /intel/mock/*/baz: {}
    config:
      /intel/mock:
        user: "root"
        password: "secret"
    tags:
      /intel/mock:
        experiment: "experiment 11"
      /intel/mock/bar:
        os: "linux"
    process:
      -
        plugin_name: "passthru"
        publish:
          -
            plugin_name: "file"
            config:
              file: "/tmp/published"
```

The workflow is a [DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph) which describes the how and what of a task.  It is always rooted by a `collect`, and then contains any number of `process`es and `publish`es.

#### Remote Targets

Process and Publish nodes in the workflow can also target remote Snap nodes via the 'target' key. The purpose of this is to allow offloading of resource intensive workflow steps from the node where data collection is occurring. Modifying the example above we have:

```yaml
---
  collect:
    metrics:
      /intel/mock/foo: {}
      /intel/mock/bar: {}
      /intel/mock/*/baz: {}
    config:
      /intel/mock:
        user: "root"
        password: "secret"
    tags:
      /intel/mock:
        experiment: "experiment 11"
      /intel/mock/bar:
        os: "linux"
    process:
      -
        plugin_name: "passthru"
        target: "127.0.0.1:8082"
        publish:
          -
            plugin_name: "file"
            target: "127.0.0.1:8082"
            config:
              file: "/tmp/published"

```

If a target is specified for a step in the workflow, that step will be executed on the remote instance specified by the ip:port target. Each node in the workflow is evaluated independently so a workflow can have any, all, or none of its steps being done remotely (if `target` key is omitted, that step defaults to local). The ip and port target are the ip and port that has a running control-grpc server. These can be specified to snapteld via the `control-listen-addr` and `control-listen-port` flags. The default is the same ip as the Snap rest-api and port 8082.

An example json task that uses remote targets:
```json
{
  "version": 1,
  "schedule": {
    "type": "simple",
    "interval": "1s"
  },
  "max-failures": 10,
  "workflow": {
    "collect": {
      "metrics": {
        "/intel/mock/foo": {},
        "/intel/mock/bar": {},
        "/intel/mock/*/baz": {}
      },
      "config": {
        "/intel/mock": {
          "user": "root",
          "password": "secret"
        }
      },
      "process": [
        {
          "plugin_name": "passthru",
          "target": "127.0.0.1:9999",
          "process": null,
          "publish": [
            {
              "plugin_name": "file",
              "target": "127.0.0.1:9992",
              "config": {
                "file": "/tmp/snap_published_mock_file.log"
              }
            }
          ]
        }
      ]
    }
  }
}

```

More information about the architecture behind this can be found [here](DISTRIBUTED_WORKFLOW_ARCHITECTURE.md).

#### collect

The collect section describes which metrics are requested to be collected.

Metrics can be enumerated using:

 a) **concrete _namespace_**

Declaring a metric's name exactly as it appears in the metric catalog (see `snaptel metric list`).

    Metrics requested in task manifest          | Collected metrics
    --------------------------------------------|------------------------
    /intel/mock/foo                             |  /intel/mock/foo
	|
    /intel/mock/bar                             |  /intel/mock/bar
	|
    /intel/mock/\*/baz <br/> _(dynamic metric)_ |  /intel/mock/host0/baz <br/> /intel/mock/host1/baz <br/> /intel/mock/host2/baz  <br/> /intel/mock/host3/baz  <br/> /intel/mock/host4/baz <br/> /intel/mock/host5/baz <br/> /intel/mock/host6/baz <br/> /intel/mock/host7/baz  <br/> /intel/mock/host8/baz <br/> /intel/mock/host9/baz <br/><br/> _(collect metrics for all instances of the dynamic metric)_

 
 
 b) **_specified_ _instance_ of dynamic metrics**

Specifying a dynamic metric refers to providing a `value` in place of the dynamic element in the namespace (e.g. hostname, cgroup id, etc.). It's important to remember that dynamic elements are represented by an asterisk when presented in the metric catalog.
When a task manifest contains a specific instance of a dynamic metric only that instance will be collected. If the value does not exist the task will error.

 Metrics requested in task manifest | Collected metrics
------------------------------------|------------------------
/intel/mock/host0/baz <br/><br/> _(specific instance of "/intel/mock/*/baz")_ |  /intel/mock/host0/baz <br/><br/> _(only this one metric will be collected)_


 c) **dynamic _query_**

Dynamic queries are those that contain:

- **wildcards** `*` - that matches with any value in the metric namespace or, if the wildcard is in the end, with all metrics with the given prefix
- and/or **tuples of values** `(x;y;z)` - that matches with all items separated by semicolon and works like logical _and_, so it gives an error if even one of these items cannot be collected

Metrics requested in task manifest  | Collected metrics
------------------------------------|------------------------
/intel/mock/*                       | /intel/mock/foo <br/> /intel/mock/bar <br/> /intel/mock/host0/baz <br/> /intel/mock/host1/baz <br/> /intel/mock/host2/baz  <br/> /intel/mock/host3/baz  <br/> /intel/mock/host4/baz <br/> /intel/mock/host5/baz <br/> /intel/mock/host6/baz <br/> /intel/mock/host7/baz  <br/> /intel/mock/host8/baz <br/> /intel/mock/host9/baz <br/> <br/> _(collect all metrics with prefix "/intel/mock/")_
|
/intel/mock/(foo;bar)               | /intel/mock/foo <br/> /intel/mock/bar
|
/intel/mock/(host0;host1;host2)/baz | /intel/mock/host0/baz <br/> /intel/mock/host1/baz <br/> /intel/mock/host2/baz <br/>

The namespaces are keys to another nested object which may contain a specific version of a plugin, e.g.:


```yaml
---
/foo/bar/baz:
  version: 4
```

If a version is not given, Snap will __select__ the latest for you.

The config section describes configuration data for metrics.  Since metric namespaces form a tree, config can be described at a branch, and all leaves of that branch will receive the given config.  For example, say a task is going to collect `/intel/perf/foo`, `/intel/perf/bar`, and `/intel/perf/baz`, all of which require a username and password to collect.  That config could be described like so:

```yaml
---
metrics:
  /intel/perf/foo: {}
  /intel/perf/bar: {}
  /intel/perf/baz: {}
config:
  /intel/perf:
    username: jerr
    password: j3rr
```

Applying the config at `/intel/perf` means that all leaves of `/intel/perf` (`/intel/perf/foo`, `/intel/perf/bar`, and `/intel/perf/baz` in this case) will receive the config.

The tag section describes additional meta data for metrics.  Similar to config, tags can also be described at a branch, and all leaves of that branch will receive the given tag(s).  For example, say a task is going to collect `/intel/perf/foo`, `/intel/perf/bar`, and `/intel/perf/baz`, all metrics should be tagged with experiment number, additionally one metric `/intel/perf/bar` should be tagged with OS name.  That tags could be described like so:

```yaml
---
metrics:
  /intel/perf/foo: {}
  /intel/perf/bar: {}
  /intel/perf/baz: {}
tags:
  /intel/perf:
    experiment: "experiment 11"
  /intel/perf/bar:
    os: "linux"
```

Applying the tags at `/intel/perf` means that all leaves of `/intel/perf` (`/intel/perf/foo`, `/intel/perf/bar`, and `/intel/perf/baz` in this case) will receive the tag `experiment: experiment 11`.
Applying the tags at `/intel/perf/bar` means that only `/intel/perf/bar` will receive the tag `os: linux`.

A collect node can also contain any number of process or publish nodes.  These nodes describe what to do next.

#### process

A process node describes which plugin to use to process data coming from either a collection or another process node.  The config section describes config data which may be needed for the chosen plugin.

A process node may have any number of process or publish nodes.

#### publish

A publish node describes which plugin to use to process data coming from either a collection or a process node.  The config section describes config data which may be needed for the chosen plugin.

A publish node is a [pendant vertex (a leaf)](http://mathworld.wolfram.com/PendantVertex.html).  It may contain no collect, process, or publish nodes.

## TL;DR

Below is a complete example task.

### YAML

```yaml
---
  version: 1
  schedule:
    type: "simple"
    interval: "1s"
  workflow:
    collect:
      metrics:
        /intel/mock/foo: {}
        /intel/mock/bar: {}
        /intel/mock/*/baz: {}
      config:
        /intel/mock:
          user: "root"
          password: "secret"
      tags:
        /intel/perf:
          experiment: "experiment 11"
        /intel/perf/bar:
          os: "linux"
      process:
        -
          plugin_name: "passthru"
          process: null
          publish:
            -
              plugin_name: "file"
              config:
                file: "/tmp/published"
```

### JSON

```json
{
    "version": 1,
    "schedule": {
        "type": "simple",
        "interval": "1s"
    },
    "workflow": {
        "collect": {
            "metrics": {
                "/intel/mock/foo": {},
                "/intel/mock/bar": {},
                "/intel/mock/*/baz": {}
            },
            "config": {
                "/intel/mock": {
                    "user": "root",
                    "password": "secret"
                }
            },
            "tags": {
                "/intel/mock": {
                    "experiment": "experiment 11"
                },
                "/intel/mock/bar": {
                    "os": "linux"
                }
            },
            "process": [
                {
                    "plugin_name": "passthru",
                    "process": null,
                    "publish": [
                        {
                            "plugin_name": "file",
                            "config": {
                                "file": "/tmp/published"
                            }
                        }
                    ]
                }
            ]
        }
    }
}
```

#### footnotes

1. YAML is only supported via the snaptel CLI.  Only JSON is accepted via the REST API.
2. The wildcard must be supported by the target plugin.
