..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _postgres-config:
.. _postgresql-config:

Configure Canton with PostgreSQL
================================

Example configuration
---------------------

This example shows a `PostgreSQL <https://https://www.postgresql.org/>`_ storage configuration for a Sequencer, Mediator, and Participant Node all running on a local PostgreSQL database instance on port ``5432``.

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/storage-postgresql.conf

Configure the connection pool
-----------------------------

Canton uses `HikariCP <https://github.com/brettwooldridge/HikariCP>`_ for connection pooling. This is configured in ``storage.config``. We recommend this article on how to `choose the size of the pool. <https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing>`_

.. note::

    The setup of ``dataSourceClassName`` and ``properties`` for PostgreSQL are discussed in the next section.

The set of pool properties that may be set is given below, the descriptions of the properties can be found in the associated ``get``/``set`` method descriptions for `HikariConfig <https://www.javadoc.io/doc/com.zaxxer/HikariCP/3.2.0/com/zaxxer/hikari/HikariConfig.html>`_.

.. list-table::
   :widths: 33 33 33

   * - allowPoolSuspension
     - catalog
     - connectionInitSql
   * - connectionTestQuery
     - connectionTimeout
     - dataSourceClassName
   * - idleTimeout
     - initializationFailTimeout
     - isolateInternalQueries
   * - leakDetectionThreshold
     - maxLifetime
     - maximumPoolSize
   * - minimumIdle
     - poolName
     - properties
   * - readOnly
     - registerMbeans
     - schema
   * - validationTimeout
     -
     -


Configure the PostgreSQL data source
------------------------------------

To create a connection ``HikariCP`` uses the *data-source* ``dataSourceClassName`` configured using the properties in ``properties``. We recommend using the ``org.postgresql.ds.PGSimpleDataSource`` *data-source* configured with the following properties:

 - serverName
 - databaseName
 - portNumber
 - user
 - password

You can find the details of additional supported properties by reviewing the associated ``get``/``set`` method descriptions for `PGSimpleDataSource <https://jdbc.postgresql.org/documentation/publicapi/org/postgresql/ds/PGSimpleDataSource.html>`_.

.. _postgresql-env-based-config:

Use environment variables in configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use environment variables to configure the PostgreSQL data-source properties. This is useful for sensitive information like passwords or when you want to avoid hardcoding values in your configuration files.

In this example all the database properties are set using environment variables. The environment variables are prefixed with ``SEQUENCER1_`` to avoid conflicts with other configurations.

.. code-block::

    config {
      dataSourceClassName = "org.postgresql.ds.PGSimpleDataSource"
      properties = {
        serverName = ${SEQUENCER1_SERVER}
        databaseName = ${SEQUENCER1_DB}
        portNumber = ${SEQUENCER1_PORT}
        user = ${SEQUENCER1_USER}
        password = ${SEQUENCER1_PASSWORD}
      }

Share database configuration across nodes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This example shows how  `PureConfig <https://github.com/pureconfig/pureconfig>`_ can be used to share common database configuration across multiple nodes in a Canton setup.

.. literalinclude:: CANTON/community/app/src/pack/config/storage/postgres.conf

.. _postgres-ssl:

Use SSL
-------

Configure SSL using the following ``PGSimpleDataSource`` properties.

ssl = true
    Verify both the SSL certificate and verify the hostname

sslmode= "verify-ca"
    Check the certificate chain up to the root certificate stored on the client.

sslrootcert = "path/to/root.cert"
    Optionally set this to set with path to root certificate.

For more details on how to configure SSL in PostgreSQL, see the `PostgreSQL SSL <https://jdbc.postgresql.org/documentation/ssl/>`_ documentation.

Use mTLS
~~~~~~~~

To configure mutual TLS (`mTLS <https://en.wikipedia.org/wiki/Mutual_authentication#mTLS>`_) you can use the following additional properties:

    - sslcert = "path/to/client-cert.pem"
    - sslkey = "path/to/client-key.p12"

Set up the PostgreSQL database
------------------------------

A separate database is required for each Canton node. Create the database before starting Canton.

.. note::

    The canton distribution provides a script, ``config/utils/postgres/db.sh``, to help create the database and users.

Create the database
~~~~~~~~~~~~~~~~~~~

Databases must be created with UTF8 encoding to ensure proper handling of Unicode characters. The following SQL command creates a database named ``participant1_db`` with UTF8 encoding:

.. code-block:: sql

    create database participant1_db encoding = 'UTF8';

Create a database user
~~~~~~~~~~~~~~~~~~~~~~

The database user configured in the *data-source* properties must have the necessary permissions to create and modify the database schema, in addition to reading and writing data.

The following SQL commands create a user named ``participant1_user`` with a password and grant all privileges on the database:

.. code-block:: sql

    create user participant1_user with password 'change-me';
    grant all privileges on database participant1_db to participant1_user;

Operations
----------

Optimize storage
~~~~~~~~~~~~~~~~

See :ref:`Storage Optimization <optimize_storage>`.

Backup
~~~~~~

See :ref:`Backup and Restore <backup-and-restore>`.

Setup HA
~~~~~~~~

See :ref:`High Availability Usage <ha_user_manual>`.

Use a cloud hosted database
---------------------------

You can use a cloud-hosted PostgreSQL database, such as `Amazon RDS <https://aws.amazon.com/rds/postgresql/>`_, `Google Cloud SQL <https://cloud.google.com/sql/docs/postgres>`_,
or `Azure Database for PostgreSQL <https://azure.microsoft.com/en-us/services/postgresql/>`_.

Please refer to the documentation of the respective cloud provider for details on how to set up and configure and secure a PostgreSQL database.
