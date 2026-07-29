// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.data

import com.digitalasset.canton.InternedPartyId
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  InternedParticipantId,
  PartyAndOrder,
}
import com.digitalasset.canton.resource.DbStorage.Implicits.setParameterArrayFromToDbPrimitive
import com.digitalasset.canton.resource.{DbStorage, ToDbPrimitive}
import slick.jdbc.{GetResult, SetParameter}

import scala.reflect.ClassTag

sealed trait DbAcsDigestJournalImplicits[K] {
  implicit def classTag: ClassTag[K]
  implicit def getResultKey: GetResult[K]
  implicit def setParamKey: SetParameter[K]
  implicit def setParamArrayKey: SetParameter[Array[K]]

  def toKeysArray(iterableK: Iterable[K]): Array[K] =
    iterableK.toArray
}

object DbAcsDigestJournalImplicits {
  final case class PartyJournalImplicits(storage: DbStorage)
      extends DbAcsDigestJournalImplicits[PartyAndOrder[InternedPartyId]] {

    override implicit val classTag: ClassTag[PartyAndOrder[InternedPartyId]] =
      ClassTag(classOf[PartyAndOrder[InternedPartyId]])

    override implicit val getResultKey: GetResult[PartyAndOrder[InternedPartyId]] =
      GetResult[PartyAndOrder[InternedPartyId]] { pr =>
        val encoded = pr.nextInt()
        PartyAndOrder.decodePartyAndOrder(encoded)
      }

    override implicit val setParamKey: SetParameter[PartyAndOrder[InternedPartyId]] =
      SetParameter[PartyAndOrder[InternedPartyId]] { (key, pp) =>
        val encoded = PartyAndOrder.encodePartyAndOrder(key)
        pp.setInt(encoded)
      }

    override implicit val setParamArrayKey: SetParameter[Array[PartyAndOrder[InternedPartyId]]] =
      setParameterArrayFromToDbPrimitive(
        toDbPrimitive = ToDbPrimitive(v => PartyAndOrder.encodePartyAndOrder(v)),
        setArrayParameter = storage.DbStorageConverters.setParameterArrayInt,
        ct = implicitly[ClassTag[Int]],
      )
  }

  final case class ParticipantJournalImplicits(storage: DbStorage)
      extends DbAcsDigestJournalImplicits[InternedParticipantId] {

    override implicit val classTag: ClassTag[InternedParticipantId] =
      ClassTag(classOf[InternedParticipantId])

    override implicit val getResultKey: GetResult[InternedParticipantId] =
      GetResult[InternedParticipantId] { pr =>
        pr.nextInt()
      }

    override implicit val setParamKey: SetParameter[InternedParticipantId] =
      SetParameter[InternedParticipantId] { (key, pp) =>
        pp.setInt(key)
      }

    override implicit val setParamArrayKey: SetParameter[Array[InternedParticipantId]] =
      storage.converters.setParameterArrayInt
  }
}
