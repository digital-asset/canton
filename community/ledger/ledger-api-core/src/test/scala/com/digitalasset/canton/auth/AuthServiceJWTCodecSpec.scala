// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.daml.jwt.{
  AuthServiceJWTCodec,
  AuthServiceJWTPayload,
  PartyJWTPayload,
  StandardJWTPayload,
  StandardJWTTokenFormat,
}
import io.circe.*
import io.circe.syntax.*
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.TryValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import java.time.Instant
import scala.util.{Success, Try}

@SuppressWarnings(Array("com.digitalasset.canton.TryFailed"))
class AuthServiceJWTCodecSpec
    extends AnyWordSpec
    with Matchers
    with TryValues
    with ScalaCheckDrivenPropertyChecks {

  val extraScopes = s"dummy-scope1 ${AuthServiceJWTCodec.scopeLedgerApiFull} dummy-scope2"
  val extraScopesJsArray = extraScopes.split(" ").toSeq.asJson.noSpaces

  /** Serializes a [[AuthServiceJWTPayload]] to JSON, then parses it back to a AuthServiceJWTPayload
    */
  private def serializeAndParse(value: AuthServiceJWTPayload)(implicit
      encoder: Encoder[AuthServiceJWTPayload],
      decoder: Decoder[AuthServiceJWTPayload],
  ): Try[AuthServiceJWTPayload] =
    for {
      serialized <- Try(value.asJson.noSpaces)
      parsed <- parse(serialized)
    } yield parsed

  /** Parses a [[AuthServiceJWTPayload]] */
  private def parse(serialized: String)(implicit
      decoder: Decoder[AuthServiceJWTPayload]
  ): Try[AuthServiceJWTPayload] =
    Try(parser.decode(serialized).fold(throw _, identity))

  private implicit val arbInstant: Arbitrary[Instant] =
    Arbitrary {
      for {
        seconds <- Gen.chooseNum(Instant.MIN.getEpochSecond, Instant.MAX.getEpochSecond)
      } yield {
        Instant.ofEpochSecond(seconds)
      }
    }

  private implicit val arbFormat: Arbitrary[StandardJWTTokenFormat] =
    Arbitrary(
      Gen.oneOf[StandardJWTTokenFormat](
        StandardJWTTokenFormat.Audience,
        StandardJWTTokenFormat.Scope,
      )
    )

  // participantId is mandatory for the format `StandardJWTTokenFormat.Audience`
  private val StandardJWTPayloadGen =
    Gen
      .resultOf((StandardJWTPayload.apply _).tupled)
      .filterNot { payload =>
        payload.participantId
          .forall(_.isEmpty) && payload.format == StandardJWTTokenFormat.Audience
      }
      .filterNot { payload =>
        payload.scope.forall(_.isEmpty) && payload.format == StandardJWTTokenFormat.Scope
      }
      // we do not fill audiences for Scope or Audience based tokens
      .map(payload => payload.copy(audiences = List.empty))
      // we coerce all scopes to contain the official ledger api string, we test the non-conforming ones separately
      .map(payload =>
        payload.copy(scope = payload.scope.map(_ => AuthServiceJWTCodec.scopeLedgerApiFull))
      )
      // filter out accidental self-signed JWTs
      .filterNot(payload => payload.issuer.contains(payload.userId))

  "Audience-Based AuthServiceJWTPayload codec" when {
    import AuthServiceJWTCodec.AudienceBasedTokenJsonImplicits.*

    val PayloadGen = Gen
      .resultOf((StandardJWTPayload.apply _).tupled)
      .map(payload =>
        payload.copy(
          participantId = None,
          format = StandardJWTTokenFormat.Audience,
          scope = payload.scope.map(_.trim()),
        )
      )

    "serializing and parsing a value" should {
      "work for arbitrary custom Daml token values" in forAll(
        PayloadGen,
        minSuccessful(100),
      )(value => serializeAndParse(value) shouldBe Success(value))
    }

    "support multiple audiences with a single participant audience" in {
      val serialized =
        """{
          |  "aud": ["https://example.com/non/related/audience",
          |          "https://daml.com/jwt/aud/participant/someParticipantId"],
          |  "sub": "someUserId",
          |  "exp": 100
          |}
        """.stripMargin
      parse(serialized) shouldBe Success(
        StandardJWTPayload(
          issuer = None,
          participantId = None,
          userId = "someUserId",
          exp = Some(Instant.ofEpochSecond(100)),
          format = StandardJWTTokenFormat.Audience,
          audiences = List(
            "https://example.com/non/related/audience",
            "https://daml.com/jwt/aud/participant/someParticipantId",
          ),
          scope = None,
        )
      )
    }
  }

  "Scope-Based AuthServiceJWTPayload codec" when {
    import AuthServiceJWTCodec.ScopeBasedTokenJsonImplicits.*

    val PayloadGen = Gen
      .resultOf((StandardJWTPayload.apply _).tupled)
      .map(payload =>
        payload.copy(
          participantId = None,
          format = StandardJWTTokenFormat.Scope,
          scope = payload.scope.map(_.trim()),
        )
      )

    "serializing and parsing a value" should {
      "work for arbitrary custom Daml token values" in forAll(
        PayloadGen,
        minSuccessful(100),
      )(value => serializeAndParse(value) shouldBe Success(value))
    }

    val expected = StandardJWTPayload(
      issuer = Some("issuer"),
      participantId = None,
      userId = "someUserId",
      exp = Some(Instant.ofEpochSecond(100)),
      format = StandardJWTTokenFormat.Scope,
      audiences = List("someParticipantId"),
      scope = Some(AuthServiceJWTCodec.scopeLedgerApiFull),
    )

    "support standard JWT claims with just one scope" in {
      val serialized =
        s"""{
           |  "iss": "issuer",
           |  "aud": "someParticipantId",
           |  "sub": "someUserId",
           |  "exp": 100,
           |  "scope": "${AuthServiceJWTCodec.scopeLedgerApiFull}"
           |}
        """.stripMargin
      parse(serialized) shouldBe Success(expected)
    }

    "support standard JWT claims with just one scope in scp" in {
      val serialized =
        s"""{
           |  "iss": "issuer",
           |  "aud": "someParticipantId",
           |  "sub": "someUserId",
           |  "exp": 100,
           |  "scp": ["${AuthServiceJWTCodec.scopeLedgerApiFull}"]
           |}
        """.stripMargin
      parse(serialized) shouldBe Success(expected)
    }

    "support standard JWT claims with extra scopes" in {
      val serialized =
        s"""{
           |  "iss": "issuer",
           |  "aud": "someParticipantId",
           |  "sub": "someUserId",
           |  "exp": 100,
           |  "scope": "$extraScopes"
           |}
        """.stripMargin
      parse(serialized) shouldBe Success(expected.copy(scope = Some(extraScopes)))
    }

    "support standard JWT claims with extra scopes in scp" in {
      val serialized =
        s"""{
           |  "iss": "issuer",
           |  "aud": "someParticipantId",
           |  "sub": "someUserId",
           |  "exp": 100,
           |  "scp": $extraScopesJsArray
           |}
        """.stripMargin
      parse(serialized) shouldBe Success(expected.copy(scope = Some(extraScopes)))
    }

    "support standard JWT claims with extra composite scopes" in {
      val serialized =
        s"""{
           |  "iss": "issuer",
           |  "aud": "someParticipantId",
           |  "sub": "someUserId",
           |  "exp": 100,
           |  "scope": "$extraScopes"
           |}
        """.stripMargin
      parse(serialized) shouldBe Success(expected.copy(scope = Some(extraScopes)))
    }

    "support standard JWT claims with extra composite scopes in scp" in {
      val serialized =
        s"""{
           |  "iss": "issuer",
           |  "aud": "someParticipantId",
           |  "sub": "someUserId",
           |  "exp": 100,
           |  "scp": $extraScopesJsArray
           |}
        """.stripMargin
      parse(serialized) shouldBe Success(expected.copy(scope = Some(extraScopes)))
    }
  }

  "AuthServiceJWTPayload codec" when {
    import AuthServiceJWTCodec.JsonImplicits.*

    "serializing and parsing a value" should {

      "work for arbitrary standard Daml token values" in forAll(
        StandardJWTPayloadGen,
        minSuccessful(100),
      ) { value =>
        serializeAndParse(value) shouldBe Success(value)
      }

      val expected = StandardJWTPayload(
        issuer = Some("issuer"),
        participantId = Some("someParticipantId"),
        userId = "someUserId",
        exp = Some(Instant.ofEpochSecond(100)),
        format = StandardJWTTokenFormat.Scope,
        audiences = List.empty,
        scope = Some(AuthServiceJWTCodec.scopeLedgerApiFull),
      )

      "support standard JWT claims with just one scope" in {
        val serialized =
          s"""{
            |  "iss": "issuer",
            |  "aud": "someParticipantId",
            |  "sub": "someUserId",
            |  "exp": 100,
            |  "scope": "${AuthServiceJWTCodec.scopeLedgerApiFull}"
            |}
          """.stripMargin
        parse(serialized) shouldBe Success(expected)
      }

      "support standard JWT claims with just one scope in scp" in {
        val serialized =
          s"""{
             |  "iss": "issuer",
             |  "aud": "someParticipantId",
             |  "sub": "someUserId",
             |  "exp": 100,
             |  "scp": ["${AuthServiceJWTCodec.scopeLedgerApiFull}"]
             |}
          """.stripMargin
        parse(serialized) shouldBe Success(expected)
      }

      "reject standard JWT claims with one composite scope" in {
        val serialized =
          s"""{
            |  "iss": "issuer",
            |  "aud": "someParticipantId",
            |  "sub": "someUserId",
            |  "exp": 100,
            |  "scope": "resource_server/${AuthServiceJWTCodec.scopeLedgerApiFull}"
            |}
          """.stripMargin
        parse(serialized).failure.exception.getMessage should include(
          "Access token with unknown scope"
        )
      }

      "support standard JWT claims with extra scopes" in {
        val serialized =
          s"""{
            |  "aud": "someParticipantId",
            |  "sub": "someUserId",
            |  "exp": 100,
            |  "scope": "$extraScopes"
            |}
          """.stripMargin
        parse(serialized) shouldBe Success(expected.copy(issuer = None, scope = Some(extraScopes)))
      }

      "support standard JWT claims with extra scopes in scp" in {
        val serialized =
          s"""{
             |  "aud": "someParticipantId",
             |  "sub": "someUserId",
             |  "exp": 100,
             |  "scp": $extraScopesJsArray
             |}
          """.stripMargin
        parse(serialized) shouldBe Success(expected.copy(issuer = None, scope = Some(extraScopes)))
      }

      "support standard JWT claims with extra composite scopes" in {
        val serialized =
          s"""{
            |  "aud": "someParticipantId",
            |  "sub": "someUserId",
            |  "exp": 100,
            |  "scope": "resource_server/dummy-scope1 ${AuthServiceJWTCodec.scopeLedgerApiFull} resource_server/dummy-scope2"
            |}
          """.stripMargin
        parse(serialized) shouldBe Success(
          expected.copy(
            issuer = None,
            scope =
              Some("resource_server/dummy-scope1 daml_ledger_api resource_server/dummy-scope2"),
          )
        )
      }

      "support standard JWT claims with extra composite scopes in scp" in {
        val serialized =
          s"""{
             |  "aud": "someParticipantId",
             |  "sub": "someUserId",
             |  "exp": 100,
             |  "scp": ["resource_server/dummy-scope1", "${AuthServiceJWTCodec.scopeLedgerApiFull}", "resource_server/dummy-scope2"]
             |}
          """.stripMargin
        parse(serialized) shouldBe Success(
          expected.copy(
            issuer = None,
            scope =
              Some("resource_server/dummy-scope1 daml_ledger_api resource_server/dummy-scope2"),
          )
        )
      }

      "support standard JWT claims with iss claim as string" in {
        val serialized =
          s"""{
            |  "iss": "issuer1",
            |  "sub": "someUserId",
            |  "scope": "${AuthServiceJWTCodec.scopeLedgerApiFull}"
            |}
          """.stripMargin
        val expected = StandardJWTPayload(
          issuer = Some("issuer1"),
          participantId = None,
          userId = "someUserId",
          exp = None,
          format = StandardJWTTokenFormat.Scope,
          audiences = List.empty,
          scope = Some(AuthServiceJWTCodec.scopeLedgerApiFull),
        )
        parse(serialized) shouldBe Success(expected)
      }

      "support standard JWT claims with iss claim as URL" in {
        val serialized =
          s"""{
            |  "iss": "http://daml.com/",
            |  "sub": "someUserId",
            |  "scope": "${AuthServiceJWTCodec.scopeLedgerApiFull}"
            |}""".stripMargin
        val expected = StandardJWTPayload(
          issuer = Some("http://daml.com/"),
          participantId = None,
          userId = "someUserId",
          exp = None,
          format = StandardJWTTokenFormat.Scope,
          audiences = List.empty,
          scope = Some(AuthServiceJWTCodec.scopeLedgerApiFull),
        )
        parse(serialized) shouldBe Success(expected)
      }

      "have stable default values" in {
        val serialized =
          s"""{
            |  "sub": "someUserId",
            |  "scope": "${AuthServiceJWTCodec.scopeLedgerApiFull}"
            |}""".stripMargin
        val expected = StandardJWTPayload(
          issuer = None,
          userId = "someUserId",
          participantId = None,
          exp = None,
          format = StandardJWTTokenFormat.Scope,
          audiences = List.empty,
          scope = Some(AuthServiceJWTCodec.scopeLedgerApiFull),
        )
        parse(serialized) shouldBe Success(expected)
      }

      "support additional daml user token with prefixed audience" in {
        val serialized =
          """{
            |  "aud": "https://daml.com/jwt/aud/participant/someParticipantId",
            |  "sub": "someUserId",
            |  "exp": 100
            |}
          """.stripMargin
        val expected = StandardJWTPayload(
          issuer = None,
          participantId = Some("someParticipantId"),
          userId = "someUserId",
          exp = Some(Instant.ofEpochSecond(100)),
          format = StandardJWTTokenFormat.Audience,
          audiences = List.empty,
          scope = None,
        )
        parse(serialized) shouldBe Success(expected)
      }

      "treat a singleton array of audiences equivalent to a string of its first element" in {
        val prefixed =
          """{
            |  "aud": ["https://daml.com/jwt/aud/participant/someParticipantId"],
            |  "sub": "someUserId",
            |  "exp": 100
            |}
          """.stripMargin
        parse(prefixed) shouldBe Success(
          StandardJWTPayload(
            issuer = None,
            participantId = Some("someParticipantId"),
            userId = "someUserId",
            exp = Some(Instant.ofEpochSecond(100)),
            format = StandardJWTTokenFormat.Audience,
            audiences = List.empty,
            scope = None,
          )
        )

        val standard =
          s"""{
            |  "aud": ["someParticipantId"],
            |  "sub": "someUserId",
            |  "exp": 100,
            |  "scope": "dummy-scope1 ${AuthServiceJWTCodec.scopeLedgerApiFull} dummy-scope2"
            |}
          """.stripMargin
        parse(standard) shouldBe Success(
          StandardJWTPayload(
            issuer = None,
            participantId = Some("someParticipantId"),
            userId = "someUserId",
            exp = Some(Instant.ofEpochSecond(100)),
            format = StandardJWTTokenFormat.Scope,
            audiences = List.empty,
            scope = Some("dummy-scope1 daml_ledger_api dummy-scope2"),
          )
        )
      }

      "support additional daml user token with prefixed audience and provided scope" in {
        val serialized =
          s"""{
            |  "aud": ["https://daml.com/jwt/aud/participant/someParticipantId"],
            |  "sub": "someUserId",
            |  "exp": 100,
            |  "scope": "${AuthServiceJWTCodec.scopeLedgerApiFull}"
            |}
          """.stripMargin
        val expected = StandardJWTPayload(
          issuer = None,
          participantId = Some("someParticipantId"),
          userId = "someUserId",
          exp = Some(Instant.ofEpochSecond(100)),
          format = StandardJWTTokenFormat.Audience,
          audiences = List.empty,
          scope = Some(AuthServiceJWTCodec.scopeLedgerApiFull),
        )
        parse(serialized) shouldBe Success(expected)
      }

      "support multiple audiences with a single participant audience" in {
        val serialized =
          """{
            |  "aud": ["https://example.com/non/related/audience",
            |          "https://daml.com/jwt/aud/participant/someParticipantId"],
            |  "sub": "someUserId",
            |  "exp": 100
            |}
          """.stripMargin
        parse(serialized) shouldBe Success(
          StandardJWTPayload(
            issuer = None,
            participantId = Some("someParticipantId"),
            userId = "someUserId",
            exp = Some(Instant.ofEpochSecond(100)),
            format = StandardJWTTokenFormat.Audience,
            audiences = List.empty,
            scope = None,
          )
        )
      }

      "reject the token of ParticipantId format with multiple participant audiences" in {
        val serialized =
          """{
            |  "aud": ["https://daml.com/jwt/aud/participant/someParticipantId",
            |          "https://daml.com/jwt/aud/participant/someParticipantId2"],
            |  "sub": "someUserId",
            |  "exp": 100
            |}
          """.stripMargin
        parse(serialized).failure.exception.getMessage should include(
          "must include a single participantId value prefixed by"
        )
      }

      "reject the token of Scope format with multiple audiences" in {
        val serialized =
          s"""{
            |  "aud": ["someParticipantId",
            |          "someParticipantId2"],
            |  "sub": "someUserId",
            |  "exp": 100,
            |  "scope": "${AuthServiceJWTCodec.scopeLedgerApiFull}"
            |}
          """.stripMargin
        parse(serialized).failure.exception.getMessage should include(
          "`aud` must be empty or a single participantId."
        )
      }

      "reject the ParticipantId format token with empty participantId" in {
        val serialized =
          """{
            |  "aud": ["https://daml.com/jwt/aud/participant/"],
            |  "sub": "someUserId",
            |  "exp": 100
            |}
          """.stripMargin
        parse(serialized).failure.exception.getMessage should include(
          "must include participantId value prefixed by"
        )
      }

      "reject token with invalid scope" in {
        val serialized =
          """{
            |  "iss": "issuer",
            |  "aud": "someParticipantId",
            |  "sub": "someUserId",
            |  "exp": 100,
            |  "scope": "resource-server/daml-ledger-api"
            |}
          """.stripMargin
        parse(serialized).failure.exception.getMessage should include(
          "Access token with unknown scope"
        )
      }
      "support standard JWT claims with sub containing all supported characters" in {
        val userId = "someUserId@^$.!`-#+'~_|:()"
        val serialized =
          s"""{
            |  "iss": "issuer",
            |  "aud": "someParticipantId",
            |  "sub": "$userId",
            |  "exp": 100,
            |  "scope": "${AuthServiceJWTCodec.scopeLedgerApiFull}"
            |}
          """.stripMargin
        parse(serialized) shouldBe Success(expected.copy(userId = userId))
      }
    }

    "Party JWTs" should {
      import AuthServiceJWTCodec.JsonImplicits.*

      val parseSuccess = (serialized: String, expected: PartyJWTPayload) => {
        parse(serialized) shouldBe Success(expected)
        serializeAndParse(expected) shouldBe Success(expected)
      }

      "support expected JWT claims" in {
        val expected = PartyJWTPayload(
          partyId = "somePartyId",
          participantId = "someParticipantId",
          userId = "someUserId",
          exp = Some(Instant.ofEpochSecond(100)),
          scope = Some(AuthServiceJWTCodec.scopeLedgerApiFull),
          synchronizerId = "someSynchronizerId",
        )

        val serialized =
          s"""{
              |  "iss": "somePartyId",
              |  "aud": "someParticipantId",
              |  "sub": "somePartyId",
              |  "exp": 100,
              |  "scope": "${AuthServiceJWTCodec.scopeLedgerApiFull}",
              |  "daml.com": {
              |    "usr": "someUserId",
              |    "syn": "someSynchronizerId"
              |  }
              |}
            """.stripMargin
        parseSuccess(serialized, expected)
      }

      "reject token without synchronizer" in {
        val serialized =
          s"""{
              |  "iss": "somePartyId",
              |  "aud": "someParticipantId",
              |  "sub": "somePartyId",
              |  "exp": 100,
              |  "scope": "${AuthServiceJWTCodec.scopeLedgerApiFull}",
              |  "daml.com": {
              |    "usr": "someUserId"
              |  }
              |}
            """.stripMargin
        parse(serialized).failure.exception.getMessage should include(
          "Expecting synchronizer ID"
        )
      }

      "reject token without user" in {
        val serialized =
          s"""{
              |  "iss": "somePartyId",
              |  "aud": "someParticipantId",
              |  "sub": "somePartyId",
              |  "exp": 100,
              |  "scope": "${AuthServiceJWTCodec.scopeLedgerApiFull}",
              |  "daml.com": {
              |    "syn": "someSynchronizerId"
              |  }
              |}
            """.stripMargin
        parse(serialized).failure.exception.getMessage should include(
          "Expecting user ID"
        )
      }
    }

    "guessing the user id" should {
      val participantAudience = "https://daml.com/jwt/aud/participant/someParticipantId"

      "yield the sub of a token with a participant audience" in {
        AuthServiceJWTCodec.guessUserId(
          s"""{"aud": "$participantAudience", "sub": "someUserId"}"""
        ) shouldBe Some("someUserId")
      }

      "yield the sub of a token with a custom audience and scope" in {
        AuthServiceJWTCodec.guessUserId(
          """{"aud": "someTargetAudience", "scope": "someTargetScope", "sub": "someUserId"}"""
        ) shouldBe Some("someUserId")
      }

      "yield no user id for a token without a sub" in {
        AuthServiceJWTCodec.guessUserId(
          """{"aud": "someTargetAudience", "scope": "someTargetScope"}"""
        ) shouldBe None
        AuthServiceJWTCodec.guessUserId(s"""{"aud": "$participantAudience"}""") shouldBe None
      }

      "yield the sub of a token that only the audience-based reader can read" in {
        // A self-issued token whose daml.com block holds a non-string user id makes the general
        // reader throw
        AuthServiceJWTCodec.guessUserId(
          """{"iss": "someUserId", "sub": "someUserId", "aud": "someTargetAudience", "daml.com": {"usr": 1}}"""
        ) shouldBe Some("someUserId")
      }

      "yield no user id for a payload that is not a JSON object" in {
        AuthServiceJWTCodec.guessUserId("not json") shouldBe None
        AuthServiceJWTCodec.guessUserId("[]") shouldBe None
      }
    }
  }
}
