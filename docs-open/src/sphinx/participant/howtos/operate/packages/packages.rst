..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _manage-daml-packages-and-archives:

Manage Daml packages and archives
=================================

About
-----

A package is a unit of compiled Daml code corresponding to one Daml project. This package contains compiled Daml-LF code. Every package has a unique `package-id`. A Daml Archive (DAR) file consists of a main package along with all other packages on which the main package depends. As the DAR only has one main package the main `package-id` can also be used to refer to the DAR. App providers distribute apps as a collection of DARs.

Manage DARs
-----------

Upload a DAR
~~~~~~~~~~~~

To upload a DAR use the :ref:`dars.upload <dars.upload>` console command.

.. snippet:: packages
    .. success:: participant2.dars.upload("dars/CantonExamples.dar", vetAllPackages = false)

All Participant Nodes running the app must have the app DARs loaded independently as they aren't shared.

List DARs
~~~~~~~~~

To list DARs use the :ref:`dars.list <dars.list>` console command.

.. snippet:: packages
    .. success:: participant2.dars.list()

.. note::

    Please note that the package **canton-builtin-admin-workflow-ping** is a package that ships with Canton. It contains the Daml templates
    used by the ``participant.health.ping`` command.


To filter the list of package provide a criteria to the ``list`` method. For example, to extract the description
of the DAR named **CantonExamples**, first filter by name and then use ``head`` to extract the first element of the list:

.. snippet:: packages
    .. success:: val cantonExampleDar = participant2.dars.list(filterName = "CantonExamples").head

Display the contents of a DAR
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To display the contents of a DAR use the :ref:`dars.get_contents <dars.get_contents>` console command.

.. snippet:: packages
    .. success(output=8):: val darContent = participant2.dars.get_contents(cantonExampleDar.mainPackageId)

The content includes the name and version of the DAR, as well as the names and versions of the packages included in the
DAR as dependencies.

.. note::

    All DAR files include packages from the Daml standard library (prefixed with **daml-**), for
    example **daml-prim**.

.. _package_vetting:

Manage package vetting
----------------------

On command submission all Participant Nodes involved in the transaction must have the :externalref:`packages necessary to interpret the transaction <package-vetting-overview>` such that they come to the same conclusion. Package vetting topology information is the way that Participant Nodes communicate to other Participant Nodes what packages they support. Uploading a DAR file to a Participant Node publishes vetting information to all connected Synchronizers making it available to other Participant Nodes. Use the ``topology.vetted_packages.list`` console command to list vetted packages.

To display the Synchronizer/Participant Node vetting state for a specific package filter the results as shown:

.. Need to boot the Synchronizer to demonstate this
.. snippet:: packages
    .. hidden:: val mySynchronizer = bootstrap.synchronizer(synchronizerName = "da", sequencers = Seq(sequencer1), mediators = Seq(mediator1), synchronizerOwners = Seq(sequencer1, mediator1), synchronizerThreshold = PositiveInt.one, staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest))
    .. hidden:: participant1.synchronizers.connect_local(sequencer1, "mysynchronizer")
    .. hidden:: participant2.synchronizers.connect_local(sequencer1, "mysynchronizer")

.. snippet:: packages
    .. success:: participant1.dars.upload("dars/CantonExamples.dar")
    .. success:: participant2.dars.upload("dars/CantonExamples.dar")
    .. hidden:: utils.synchronize_topology()
    .. success:: val mainPackageId = participant2.dars.list(filterName = "CantonExamples").head.mainPackageId

.. todo::
    `#25944: Provide pretty printed macro for filter/map operations below <https://github.com/DACH-NY/canton/issues/25944>`_

.. snippet:: packages
    .. success:: participant1.topology.vetted_packages.list().filter(_.item.packages.exists(_.packageId == mainPackageId)).map(r => (r.context.storeId, r.item.participantId))

.. note::

    In the output the ``store`` called ``Authorized`` is local to the Participant Node and is not a Synchronizer store.

If all package vetting requirements aren't satisfied by any Synchronizer, the transaction submission fails returning a ``FAILED_PRECONDITION/PACKAGE_SELECTION_FAILED`` error message.

Unvet the main DAR package
~~~~~~~~~~~~~~~~~~~~~~~~~~

To remove support for the main DAR package use the :ref:`dars.vetting.disable <dars.vetting.disable>` console command.

.. snippet:: packages
    .. success:: participant2.dars.vetting.disable(mainPackageId)
    .. hidden:: utils.synchronize_topology()
    .. success:: participant1.topology.vetted_packages.list().filter(_.item.packages.exists(_.packageId == mainPackageId)).map(r => (r.context.storeId, r.item.participantId))

.. warning::

    It's not possible to unvet a package referenced by active contracts. Attempting this results in a ``FAILED_PRECONDITION/TOPOLOGY_PACKAGE_ID_IN_USE`` error.

Use :ref:`dars.vetting.enable <dars.vetting.enable>` console command to re-enable the vetting of a package.

Validate a DAR
--------------

Use DAR validation to ensure that the packages contained within the DAR are compatible with the packages already loaded and vetted on a specific synchronizer. For example it's invalid for a new template to redefine an existing template that has the same package, module, and template names. Because this endpoint validates the DAR for compatibility with the current vetting state, a synchronizer must be connected to the participant.

By default DARs are validated on upload. To independently validate a DAR use the :ref:`dars.validate <dars.validate>` console command.

.. snippet:: packages
    .. success(output=8):: participant2.dars.validate("dars/CantonExamples.dar")

.. note::

    Validating a DAR file doesn't change the Participant Node, no information is persisted.

Get package information
-----------------------

List packages
~~~~~~~~~~~~~

To directly list packages, use:

.. snippet:: packages
    .. success(output=8):: participant2.packages.list()

Filter the list by passing a criteria to the ``list`` method. For example, to extract the description
of the package named **daml-prim**, first filter by name and then use ``head`` to extract the first element of the list:

.. snippet:: packages
    .. success(output=8):: val prim = participant2.packages.list(filterName = "daml-prim").head

Display the contents of a package
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To inspect packages use the :ref:`packages.get_contents <packages.get_contents>` console command.

.. snippet:: packages
    .. success(output=8):: participant2.packages.get_contents(prim.packageId)


Find DARs that reference a package
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To find DARs that reference a package use the :ref:`packages.get_references <packages.get_references>` console command.

.. snippet:: packages
    .. success(output=8):: participant2.packages.get_references(prim.packageId)

Remove packages and DARs
------------------------

.. danger::

    Removing packages can lead to a number of issues, including:

    - Inability to use a contract originally created against the package, even if used in an upgraded context.

    - Inability perform Ledger API reporting on archived contracts in verbose mode. This is because the package is still needed to report historical contract events.

    **For this reason, you should not remove packages or DARs in production environments unless you are sure the package was never used or that all references to the package are pruned.**

Remove a DAR
~~~~~~~~~~~~

DAR removal only removes the main package associated with the DAR, not other packages.

You can remove a DAR only when:

- There are no active contracts referencing the package.

- There are no other packages that depend on the DAR package.

- The main package of the DAR is unvetted.

To remove the DAR package

.. snippet:: packages
    .. success(output=10):: val packagesBefore = participant1.packages.list().map(_.packageId).toSet
    .. success:: val mainPackageId = participant1.dars.upload("dars/CantonExamples.dar")
    .. success(output=8):: val content = participant1.dars.get_contents(mainPackageId)

Remove the DAR using the :ref:`dars.remove <dars.remove>` console command:

.. snippet:: packages
    .. success:: participant1.dars.remove(mainPackageId)

DAR removal only removes the main package associated with the DAR, not other packages.

.. snippet:: packages
    .. success(output=10):: val packageIds = content.packages.map(_.packageId).toSet.intersect(participant1.packages.list().map(_.packageId).toSet)
    .. assert:: packageIds.nonEmpty

To remove individual packages that where bundled with the DAR see the instructions below.

This line shows non **daml-** prefixed packages that aren't referenced by any other package, and so are candidates for removal:

.. snippet:: packages
    .. success(output=10):: participant1.packages.list().filter(!_.name.startsWith("daml-")).filter(p => participant1.packages.get_references(p.packageId).isEmpty)

Force the removal of a package
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. danger::

    Force removing of a package is a dangerous operation. For this reason do not force remove packages in production environments unless you are sure the package is unused and that all references to the package are pruned.

Canton also supports removing individual packages, giving the user more fine-grained control over the system. To remove packages ensure the following conditions are true:

- The package is unused. This means that there shouldn't be an active contract corresponding to the package.

- The package is unvetted. This means there shouldn't be an active vetting transaction corresponding to the package.

- The package is not required for Participant administration (for example, the package containing the **Ping** template)

Attempting to remove a package that is required results in an ``FAILED_PRECONDITION/PACKAGE_OR_DAR_REMOVAL_ERROR`` error:

.. snippet:: packages-force-removal
    .. success:: val mySynchronizer = bootstrap.synchronizer(synchronizerName = "mysynchronizer", sequencers = Seq(sequencer1), mediators = Seq(mediator1), synchronizerOwners = Seq(sequencer1, mediator1), synchronizerThreshold = PositiveInt.one, staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest))
    .. success:: participant1.synchronizers.connect_local(sequencer1, "mysynchronizer")
    .. success:: val mainPackageId = participant1.dars.upload("dars/CantonExamples.dar")
    .. hidden:: utils.retry_until_true(participant1.topology.vetted_packages.list(mySynchronizer, filterParticipant = participant1.id.filterString).exists(_.item.packages.exists(_.packageId == mainPackageId)))
    .. failure:: participant1.packages.remove(mainPackageId)

First unvet the package:

.. snippet:: packages-force-removal
    .. success:: import com.digitalasset.daml.lf.data.Ref.IdString.PackageId
    .. success(output=5)::participant1.topology.vetted_packages.propose_delta(participant1.id, store = mySynchronizer, removes = Seq(PackageId.assertFromString(mainPackageId)))

.. snippet:: packages-force-removal
    .. hidden:: utils.retry_until_true(participant1.topology.vetted_packages.list(mySynchronizer, filterParticipant = participant1.id.filterString).forall(!_.item.packages.exists(_.packageId == mainPackageId)))

Then force remove the package:

.. snippet:: packages-force-removal
    .. success:: participant1.packages.remove(mainPackageId, force = true)


