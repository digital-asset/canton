// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.admin.api.client.commands.{
  TopologyAdminCommands,
  TopologyAdminCommandsX,
}
import com.digitalasset.canton.admin.api.client.data.topologyx.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{
  ConsoleCommandResult,
  ConsoleEnvironment,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
  Helpful,
  InstanceReferenceX,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.health.admin.data.TopologyQueueStatus
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.BaseQueryX
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.store.{StoredTopologyTransactionsX, TimeQueryX}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyTransactionX.TxHash
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

class TopologyAdministrationGroupX(
    instance: InstanceReferenceX,
    topologyQueueStatus: => Option[TopologyQueueStatus],
    consoleEnvironment: ConsoleEnvironment,
    loggerFactory: NamedLoggerFactory,
) extends TopologyAdministrationGroupCommon(
      instance,
      topologyQueueStatus,
      consoleEnvironment,
      loggerFactory,
    )
    with Helpful
    with FeatureFlagFilter {

  import runner.*

  override protected def getIdCommand(): ConsoleCommandResult[UniqueIdentifier] =
    runner.adminCommand(TopologyAdminCommandsX.Init.GetId())

  @Help.Summary("Inspect all topology transactions at once")
  @Help.Group("All Transactions")
  object transactions {

    @Help.Summary("Upload signed topology transaction")
    @Help.Description(
      """Topology transactions can be issued with any topology manager. In some cases, such
      |transactions need to be copied manually between nodes. This function allows for
      |uploading previously exported topology transaction into the authorized store (which is
      |the name of the topology managers transaction store."""
    )
    def load_serialized(bytes: ByteString): Unit =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Write.AddSignedTopologyTransaction(bytes)
        )
      }

    def load(transactions: Seq[GenericSignedTopologyTransactionX]): Unit =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommandsX.Write.AddTransactions(transactions)
        )
      }

    def authorize(
        txHash: TxHash,
        mustBeFullyAuthorized: Boolean,
        signedBy: Seq[Fingerprint] = Seq.empty,
    ): ByteString = {
      ByteString.EMPTY
    }

    @Help.Summary("List all transaction")
    def list(
        filterStore: String = AuthorizedStore.filterName,
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterAuthorizedKey: Option[Fingerprint] = None,
        protocolVersion: Option[String] = None,
    ): StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX] = {
      consoleEnvironment
        .run {
          adminCommand(
            TopologyAdminCommandsX.Read.ListAll(
              BaseQueryX(
                filterStore,
                includeProposals,
                timeQuery,
                operation,
                filterSigningKey = filterAuthorizedKey.map(_.toProtoPrimitive).getOrElse(""),
                protocolVersion.map(ProtocolVersion.tryCreate),
              )
            )
          )
        }
    }

    @Help.Summary("Manage topology transaction purging", FeatureFlag.Preview)
    @Help.Group("Purge Topology Transactions")
    object purge extends Helpful {
      def list(
          filterStore: String = "",
          includeProposals: Boolean = false,
          timeQuery: TimeQueryX = TimeQueryX.HeadState,
          operation: Option[TopologyChangeOpX] = None,
          filterDomain: String = "",
          filterSigningKey: String = "",
          protocolVersion: Option[String] = None,
      ): Seq[ListPurgeTopologyTransactionXResult] = consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommandsX.Read.PurgeTopologyTransactionX(
            BaseQueryX(
              filterStore,
              includeProposals,
              timeQuery,
              operation,
              filterSigningKey,
              protocolVersion.map(ProtocolVersion.tryCreate),
            ),
            filterDomain,
          )
        )
      }

      // TODO(#11255): implement write service
    }
  }

  object domain_bootstrap {

    // TODO(#11255) break individual bits out into separate admin functions, and have this only be the default wrapper
    def generate_genesis_topology(name: String, owners: Seq[Member]) = {

      val thisNodeRootKey = Some(instance.id.uid.namespace.fingerprint)

      val codes = Set(NamespaceDelegationX.code, OwnerToKeyMappingX.code)
      // provide the root namespace delegation and owner to key mapping
      val namespace = instance.topology.transactions
        .list(filterAuthorizedKey = thisNodeRootKey)
        .result
        .map(_.transaction)
        .filter(x => codes.contains(x.transaction.mapping.code))

      // create and sign the unionspace for the domain
      val unionspaceTransaction = instance.topology.unionspaces.propose(
        serial = PositiveInt.one,
        owners.map(_.uid.namespace.fingerprint).toSet,
        threshold = PositiveInt.tryCreate(owners.size - 1),
        signedBy = thisNodeRootKey,
      )

      val domainId = DomainId(
        UniqueIdentifier(
          Identifier.tryCreate(name),
          unionspaceTransaction.transaction.mapping.unionspace,
        )
      )

      // create and sign the initial domain parameters
      val domainParameterState = instance.topology.domain_parameters.propose(
        serial = PositiveInt.one,
        domainId,
        signedBy = thisNodeRootKey,
      )

      val mediatorState = instance.topology.mediators.propose(
        serial = PositiveInt.one,
        domainId,
        threshold = PositiveInt.one,
        active = owners.collect { case id @ MediatorId(_) => id },
        signedBy = thisNodeRootKey,
      )

      val sequencerState = instance.topology.sequencers.propose(
        serial = PositiveInt.one,
        domainId,
        threshold = PositiveInt.one,
        active = owners.collect { case id @ SequencerId(_) => id },
        signedBy = thisNodeRootKey,
      )

      namespace ++ Seq(
        unionspaceTransaction,
        domainParameterState,
        sequencerState,
        mediatorState,
      )
    }
  }

  @Help.Summary("Manage unionspaces")
  @Help.Group("Unionspaces")
  object unionspaces extends Helpful {
    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterNamespace: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListUnionspaceDefinitionResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.ListUnionspaceDefinition(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterNamespace,
        )
      )
    }

    def propose(
        serial: PositiveInt,
        members: Set[Fingerprint],
        threshold: PositiveInt,
        signedBy: Option[Fingerprint] = None,
    ): SignedTopologyTransactionX[TopologyChangeOpX, UnionspaceDefinitionX] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommandsX.Write.Propose(
            serial,
            UnionspaceDefinitionX
              .create(
                UnionspaceDefinitionX.computeNamespace(members.map(Namespace(_))),
                threshold,
                members.map(Namespace(_)).toSeq,
              ),
            signedBy = signedBy.toList,
          )
        )
      }

    def join(
        unionspace: Fingerprint,
        owner: Option[Fingerprint] = Some(instance.id.uid.namespace.fingerprint),
    ): GenericSignedTopologyTransactionX = {
      ???
    }

    def leave(
        unionspace: Fingerprint,
        owner: Option[Fingerprint] = Some(instance.id.uid.namespace.fingerprint),
    ): ByteString = {
      ByteString.EMPTY
    }
  }

  @Help.Summary("Manage namespace delegations")
  @Help.Group("Namespace delegations")
  object namespace_delegations extends Helpful {

    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterNamespace: String = "",
        filterSigningKey: String = "",
        filterTargetKey: Option[Fingerprint] = None,
        protocolVersion: Option[String] = None,
    ): Seq[ListNamespaceDelegationResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.ListNamespaceDelegation(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterNamespace,
          filterTargetKey,
        )
      )
    }
  }

  @Help.Summary("Manage identifier delegations")
  @Help.Group("Identifier delegations")
  object identifier_delegations extends Helpful {

    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterUid: String = "",
        filterSigningKey: String = "",
        filterTargetKey: Option[Fingerprint] = None,
        protocolVersion: Option[String] = None,
    ): Seq[ListIdentifierDelegationResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.ListIdentifierDelegation(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterUid,
          filterTargetKey,
        )
      )
    }
  }

  // TODO(#11255) add topology commands for v2 service
  // TODO(#11255) complete @Help.Description's (by adapting TopologyAdministrationGroup-non-X descriptions)
  @Help.Summary("Manage owner to key mappings")
  @Help.Group("Owner to key mappings")
  object owner_to_key_mappings
      extends OwnerToKeyMappingsGroup(consoleEnvironment.commandTimeouts)
      with Helpful {

    @Help.Summary("List owner to key mapping transactions")
    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterKeyOwnerType: Option[KeyOwnerCode] = None,
        filterKeyOwnerUid: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListOwnerToKeyMappingResult] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommandsX.Read.ListOwnerToKeyMapping(
            BaseQueryX(
              filterStore,
              includeProposals,
              timeQuery,
              operation,
              filterSigningKey,
              protocolVersion.map(ProtocolVersion.tryCreate),
            ),
            filterKeyOwnerType,
            filterKeyOwnerUid,
          )
        )
      }

    @Help.Summary("Rotate the key for an owner to key mapping")
    def rotate_key(
        owner: KeyOwner,
        currentKey: PublicKey,
        newKey: PublicKey,
    ): Unit = ???
  }

  @Help.Summary("Manage party to participant mappings")
  @Help.Group("Party to participant mappings")
  object party_to_participant_mappings extends Helpful {
    // TODO(#11255): implement write service

    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterParty: String = "",
        filterParticipant: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListPartyToParticipantResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.ListPartyToParticipant(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterParty,
          filterParticipant,
        )
      )
    }
  }

  @Help.Summary("Manage domain trust certificates")
  @Help.Group("Domain trust certificates")
  object domain_trust_certificates extends Helpful {
    // TODO(#11255): implement write service

    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        // TODO(#11255) should be filterDomain and filterParticipant
        filterUid: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListDomainTrustCertificateResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.ListDomainTrustCertificate(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterUid,
        )
      )
    }

    // TODO(#11255) document console command
    def active(domainId: DomainId, participantId: ParticipantId): Boolean =
      list(filterStore = domainId.filterString).exists { x =>
        x.item.domainId == domainId.uid && x.item.participantId == participantId.uid
      }

  }

  @Help.Summary("Inspect participant domain states")
  @Help.Group("Participant Domain States")
  object participant_domain_permissions extends Helpful {
    // TODO(#11255): implement write service

    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterUid: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListParticipantDomainPermissionResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.ListParticipantDomainPermission(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterUid,
        )
      )
    }
  }

  @Help.Summary("Manage party hosting limits")
  @Help.Group("Party hosting limits")
  object party_hosting_limits extends Helpful {
    // TODO(#11255): implement write service

    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterUid: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListPartyHostingLimitsResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.ListPartyHostingLimits(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterUid,
        )
      )
    }
  }

  @Help.Summary("Manage package vettings")
  @Help.Group("Vetted Packages")
  object vetted_packages extends Helpful {
    // TODO(#11255): implement write service

    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterParticipant: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListVettedPackagesResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.ListVettedPackages(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterParticipant,
        )
      )
    }
  }

  @Help.Summary("Manage authority-of mappings")
  @Help.Group("Authority-of mappings")
  object authority_of extends Helpful {
    // TODO(#11255): implement write service

    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterParty: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListAuthorityOfResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.ListAuthorityOf(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterParty,
        )
      )
    }
  }

  @Help.Summary("Inspect mediator domain state")
  @Help.Group("Mediator Domain State")
  object mediators extends Helpful {
    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterDomain: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListMediatorDomainStateResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.MediatorDomainState(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterDomain,
        )
      )
    }

    def propose(
        serial: PositiveInt,
        domainId: DomainId,
        threshold: PositiveInt,
        active: Seq[MediatorId],
        passive: Seq[MediatorId] = Seq.empty,
        group: NonNegativeInt = NonNegativeInt.zero,
        signedBy: Option[Fingerprint] = None,
    ): SignedTopologyTransactionX[TopologyChangeOpX, MediatorDomainStateX] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommandsX.Write.Propose(
            serial,
            MediatorDomainStateX
              .create(domainId, group, threshold, active, passive),
            signedBy.toList,
          )
        )
      }
  }

  @Help.Summary("Inspect sequencer domain state")
  @Help.Group("Sequencer Domain State")
  object sequencers extends Helpful {
    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterDomain: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListSequencerDomainStateResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.SequencerDomainState(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterDomain,
        )
      )
    }

    def propose(
        serial: PositiveInt,
        domainId: DomainId,
        threshold: PositiveInt,
        active: Seq[SequencerId],
        passive: Seq[SequencerId] = Seq.empty,
        signedBy: Option[Fingerprint] = None,
    ): SignedTopologyTransactionX[TopologyChangeOpX, SequencerDomainStateX] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommandsX.Write.Propose(
            serial,
            SequencerDomainStateX
              .create(domainId, threshold, active, passive),
            signedBy.toList,
          )
        )
      }
  }

  @Help.Summary("Manage domain parameters state", FeatureFlag.Preview)
  @Help.Group("Domain Parameters State")
  object domain_parameters extends Helpful {
    def list(
        filterStore: String = "",
        includeProposals: Boolean = false,
        timeQuery: TimeQueryX = TimeQueryX.HeadState,
        operation: Option[TopologyChangeOpX] = None,
        filterDomain: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListDomainParametersStateResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommandsX.Read.DomainParametersState(
          BaseQueryX(
            filterStore,
            includeProposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterDomain,
        )
      )
    }

    def propose(
        serial: PositiveInt,
        domain: DomainId,
        signedBy: Option[Fingerprint] = None,
    ): SignedTopologyTransactionX[TopologyChangeOpX, DomainParametersStateX] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommandsX.Write.Propose(
            serial,
            // TODO(#11255) maybe don't just take default values for dynamic parameters
            DomainParametersStateX(domain)(
              DynamicDomainParameters.defaultValues(ProtocolVersion.dev)
            ),
            signedBy.toList,
          )
        )
      }
  }

  @Help.Summary("Inspect topology stores")
  @Help.Group("Topology stores")
  object stores extends Helpful {
    @Help.Summary("List available topology stores")
    def list(): Seq[String] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommandsX.Read.ListStores()
        )
      }
  }
}
