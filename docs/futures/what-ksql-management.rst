Managing KSQL artifacts
*******************************

As a Kafka deployment is growing it becomes natural to start processing streaming data, and we like to do it in an structured way.
This is usually done using Kafka Streams or KSQL.
So as natural, as a user, you aim to handle your KSQL queries as well from within your gitops pipeline, and you can do now this with
JulieOps since version 3.0

**IMPORTANT**: This functionality is available only since version 3.0 of JulieOps.

How does it work
-----------

Dealing with KSQL queries works as with other artefacts in your deployment, you need to first define them in your yaml, for example like:

.. code-block:: YAML

  context: "context"
  company: "company"
  env: "env"
  source: "source"
  projects:
    - name: "projectA"
      ksql:
        artefacts:
            streams:
                - path: "ksql-streams/riderlocations.sql"
                  name: "riderLocations"
            tables:
                - path: "ksql-tables/users.sql"
                  name: "users"
        access_control:
            - principal: "User:ksql0"
              topics:
                read:
                    - "topicA"
                write:
                    - "topicC"
      topics:
        - name: "foo"
        - name: "bar"
          dataType: "avro"

If you have been using JulieOps, or previously Kafka Topology Builder, you will notice a change in the YAML format.
Since the release of this feature with 3.0, you can define in the ksql section both, artefacts and access control principals.

**NOTE**: This change is implemented in backwards compatible mode, in case no header keywords is used (artefact or access_control)
the old behaviour would be expected, only principals :smile:

As with Schemas, you would need to have in your filesystem a directory with your queries, in the example it would be the directory
connectors, relative to the location of the topology file.

**NOTE**: Remember, files can be either relative to the topology file or absolute.

As with the connectors, when defining KSQL queries you would need to use a name, this will be the same as defined within the query content.


KSQL queries supported
-----------

It is currently supported to manage:

* TABLES, as described https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/create-table/ or https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/create-table-as-select/
* STREAMS, as described https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/create-stream/ or https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/create-stream-as-select/

*NOTE*: The management of pull or push queries is not currently in the scope fo this release.

Configuring KSQL servers
-----------

To manage KSQL artefacts, you must define the ksqlDB server URL to be used during the artefacts creation. This could be done like this:

.. code-block:: bash

    platform.server.ksql.url = "http://ksql:8088"

*NOTE* The URL must contain the protocol and the port.


The following additional configuration keys are optional and can be used
to configure authentication and SSL related properties:

.. code-block:: bash

    platform.server.ksql.useAlpn = false
    platform.server.ksql.truststore = "/path/to/truststore.jks"
    platform.server.ksql.truststorePw = "truststoresecret"
    platform.server.ksql.verifyHost = true
    platform.server.ksql.keystore = "/path/to/keystore.jks"
    platform.server.ksql.keystorePw = "keystoresecret"
    platform.server.ksql.user = "basic auth user"
    platform.server.ksql.password = "basic auth password"


E.g. to connect to a Confluent Cloud ksqlDB cluster, you'd use the following properties:

.. code-block:: bash

    platform.server.ksql.url = "https://<host>:443"
    platform.server.ksql.useAlpn = true
    platform.server.ksql.user = "<ksqlDB-API-key>"
    platform.server.ksql.password = "<ksqlDB-API-secret>"
