How can you install Julie Ops
*******************************

Julie Ops is available to be integrated using different artifacts:

* As a **CLI tool**, available to be installed using rpm, deb and tar.gz packages from `github <https://github.com/kafka-ops/kafka-topology-builder/releases>`_.
The rpm and deb releases are signed - if you are interested in importing the matching public key - you can download it here: `public key <https://github.com/kafka-ops/julie/blob/master/release/keys/public.key>`_.

* As a **docker image**, available from `docker hub <https://hub.docker.com/repository/docker/purbon/kafka-topology-builder>`_.

**NOTE**: In the future it will also be available as a jar to be integrated in other libraries.

Running Julie Ops as a CLI command
-----------

If you are using the CLI tool, you an use the *--help* command to list the different options available.

.. code-block:: bash

    $> julie-ops-cli.sh  --help
        usage: cli
            --brokers <arg>                  The Apache Kafka server(s) to connect to.
            --clientConfig <arg>             The client configuration file.
            --dryRun                         Print the execution plan without
                                             altering anything.
            --help                           Prints usage information.
            --overridingClientConfig <arg>   The overriding AdminClient
                                             configuration file.
            --plans <arg>                    File describing the predefined plans
            --quiet                          Print minimum status update
            --topology <arg>                 Topology config file.
            --validate                       Only run configured validations in
                                             your topology
            --version                        Prints useful version information.

The most important ones are:

* *--brokers*: This is an optional parameter where the user can list the target Kafka cluster urls.
* *--clientConfig*: As other tools, Julie Ops needs it's own configuration. In this parameter users can pass a file listing all different personalisation options.
* *--overridingClientConfig*: The user can pass a second configuration. This configuration takes priority over the default. This mechanism can be used in a CI/CD pipeline, to separate credentials from the main configuration.
* *--dryRun*: When as a user, you don't want to run the tool, but instead see what might happen. This option is very useful to evaluate changes before applying them to the cluster.
* *--topology*: This is where you will pass the topology file. It can be either a single file, or a directory. If a directory is used, all files within are going to be compiled into a single macro topology.
* *--version*: If you wanna know the version you are running.

Running Julie Ops as a Docker image.
-----------

As explained earlier, users can run the tool as well directly as docker images.
An example command for this function will look like this:

.. code-block:: bash

    $> docker run -t -i \
          -v /Users/pere/work/kafka-topology-builder/example:/example \
          purbon/kafka-topology-builder:latest \
          julie-ops-cli.sh \
          --brokers pkc-4ygn6.europe-west3.gcp.confluent.cloud:9092 \
          --clientConfig /example/topology-builder-with-schema-cloud.properties \
          --topology /example/descriptor.yaml -quiet

CLI options are all available here.
Available image tags can be found at docker hub.