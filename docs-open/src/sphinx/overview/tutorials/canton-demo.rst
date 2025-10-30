..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _demo:

Tutorials
=========


Canton
******

.. wip::
   * Update terminology in Canton demo
   * Redo the video with the new terminology
   * Link to downloading in the overview instead of directly

The Canton demo is used to demonstrate the unique Canton capabilities:

  * Application Composability - Add new workflows at any time to a running system
  * Network Interoperability - Create workflows spanning across synchronizers
  * Privacy - Canton uses data minimization and only shares data on a need-to-know basis.
  * Regulatory compliance - Canton can be used to even integrate personal sensitive information directly in workflows
    without fear of failing to be GDPR compliant.

The demo is a thin application running on top of a setup with 5 participant nodes and 2 synchronizers. You can run it by
downloading the `release package from github <https://github.com/digital-asset/daml/releases>`__. Then, unpack and
start it, using the following commands (or the zip equivalent)

.. code-block:: bash

    tar zxvf canton-open-source-x.y.z.tar.gz
    cd canton-open-source-x.y.z
    bash start-demo.command

You need to replace ``x.y.z`` with the appropriate version number of the release you've downloaded. On Windows,
you can just double-click the ``start-demo-win.cmd`` script in Windows explorer.

.. note::

    The demo requires JavaFX. Please use a Java runtime of version 11 or greater.

If you don't want to run it yourself, you can also watch our recording.

.. raw:: html

    <video id="clip" controls="controls" preload="none" onclick="this.paused ? this.play() : this.pause();" width=640 height=400 data-setup="{}">
        <source src="https://www.canton.io/videos/canton-demo.mp4" type='video/mp4'/>
    </video>


The entire code base of the demo is included in the release package as ``demo``.


The Global Synchronizer
***********************

.. todo:: <https://github.com/DACH-NY/canton/issues/25700>
   The Canton demo is built on top of the Canton console / the gRPC Ledger API.
   It therefore doesn't include the concepts of CN (Scan, SV vs. validator).
   Can we have another demo that gives an overview of those concepts?
