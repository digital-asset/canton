// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.data

import com.digitalasset.canton.InternedPartyId
import com.digitalasset.canton.participant.store.AcsDigestStore.InternedParticipantId
import com.digitalasset.canton.resource.DbStorage
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
      extends DbAcsDigestJournalImplicits[InternedPartyId] {

    override implicit val classTag: ClassTag[InternedPartyId] =
      ClassTag(classOf[InternedPartyId])

    override implicit val getResultKey: GetResult[InternedPartyId] =
      GetResult[InternedPartyId] { pr =>
        pr.nextInt()
      }

    override implicit val setParamKey: SetParameter[InternedPartyId] =
      SetParameter[InternedPartyId] { (key, pp) =>
        pp.setInt(key)
      }

    override implicit val setParamArrayKey: SetParameter[Array[InternedPartyId]] =
      storage.converters.setParameterArrayInt
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
