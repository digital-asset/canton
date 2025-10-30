..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _canton-overview:

Canton
======

.. wip::
   Trim this down to a table of contents for the Canton explanations with a bit of intro.
   Move the existing material to other sections.

   .. note::

      The Canton Polyglot Whitepaper Section 2 can be a good source of text blocks.
      https://www.canton.network/hubfs/Canton%20Network%20Files/whitepapers/Polyglot_Canton_Whitepaper_11_02_25.pdf


   Define the properties Canton achieves:

   * consensus

   * atomic transactions

   * ordering / causality / finality

   * double-spending protection

   * some notion of paying for resources and abuse prevention

   * sub-txn privacy

   * horizontal scalability of unrelated workflows (multiple synchronizers)

   * supporting multiple regulatory frameworks by design (e.g., data domicile laws)

   Explain how the subsections build on top of each other

..
   Canton is designed to fulfill its :ref:`high-level requirements. <requirements>` Before reading this section, you should be
   familiar with the Daml language and the :externalref:`hierarchical transactions <actions>` of the
   DA ledger model.


   Canton 101
   ----------

   A Basic Example
   ~~~~~~~~~~~~~~~

   We will use a simple delivery-versus-payment (DvP) example to provide
   some background on how Canton works. Alice and Bob want to exchange an IOU given
   to Alice by a bank for some shares that Bob owns. We have four parties: Alice (aka A),
   Bob (aka B), a Bank and a share registry SR. There are also three types of contracts:

   1. an Iou contract, always with Bank as the backer
   2. a Share contract, always with SR as the registry
   3. a DvP contract between Alice and Bob

   Assume that Alice has a “swap” choice on a DvP contract instance that
   exchanges an IOU she owns for a Share that Bob has. We assume that the
   IOU and Share contract instances have already been allocated in the DvP.
   Alice wishes to commit a transaction executing this swap choice; the
   transaction has the following structure:

   .. _101-dvp-example:

   .. https://www.lucidchart.com/documents/edit/cce89180-8f78-43d0-8889-799345615b7b/0
   .. image:: ./images/overview/dvp-transaction.svg
      :align: center
      :width: 80%

