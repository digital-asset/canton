// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.services.VersionFile
import com.digitalasset.canton.tracing.TraceContext
import com.softwaremill.quicklens.*
import monocle.macros.syntax.lens.*
import org.semver4j.Semver
import sttp.apispec
import sttp.apispec.asyncapi.AsyncAPI
import sttp.apispec.openapi.OpenAPI
import sttp.apispec.{Schema, SchemaLike, asyncapi, openapi}
import sttp.tapir.docs.asyncapi.AsyncAPIInterpreter
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter
import sttp.tapir.{AnyEndpoint, headers}

import scala.collection.immutable.{ListMap, SortedMap}

class ApiDocsGenerator(override protected val loggerFactory: NamedLoggerFactory)
    extends NamedLogging {

  private val releaseNote = """
    |This specification version fixes the API inconsistencies where certain fields marked as required in the spec are in fact optional.
    |If you use code generation tool based on this file, you might need to adjust the existing application code to handle those fields as optional.
    |If you do not want to change your client code, continue using the OpenAPI specification from the previous Canton 3.4 patch release.
    |""".stripMargin.trim

  /** Endpoints used for static documents generation - should match with the live endpoints
    * @see
    *   V2Routes.serverEndpoints
    */
  private val staticDocumentationEndpoints: Seq[AnyEndpoint] = {
    val services: Seq[DocumentationEndpoints] =
      Seq(
        JsCommandService,
        JsEventService,
        JsVersionService,
        JsPackageService,
        JsPartyManagementService,
        JsStateService,
        JsUpdateService,
        JsUserManagementService,
        JsIdentityProviderService,
        JsInteractiveSubmissionService,
        JsContractService,
      )
    services.flatMap(service => service.documentation)
  }

  private def supplyProtoDocs(
      initial: openapi.OpenAPI,
      proto: ProtoInfo,
      minimalCantonVersion: String,
  ): openapi.OpenAPI = {
    val updatedComponents = initial.components.map(component => supplyComponents(component, proto))
    initial
      .focus(_.components)
      .replace(updatedComponents)
      .focus(_.info.description)
      .replace(Some(s"$releaseNote\nMINIMUM_CANTON_VERSION=$minimalCantonVersion"))
  }

  private def supplyProtoDocs(
      initial: asyncapi.AsyncAPI,
      proto: ProtoInfo,
      minimalCantonVersion: String,
  ): asyncapi.AsyncAPI = {
    val updatedComponents = initial.components.map(component => supplyComponents(component, proto))
    initial
      .focus(_.components)
      .replace(updatedComponents)
      .focus(_.info.description)
      .replace(Some(s"$releaseNote\nMINIMUM_CANTON_VERSION=$minimalCantonVersion"))
  }

  private def supplyComponents(
      component: openapi.Components,
      proto: ProtoInfo,
  ): openapi.Components = {
    val updatedSchemas: ListMap[String, SchemaLike] =
      component.schemas.map(s => importSchemaLike(s, proto, findUniqueParent(s, component.schemas)))
    component.copy(schemas = updatedSchemas)
  }

  private def findUniqueParent(
      component: (String, SchemaLike),
      schemas: ListMap[String, SchemaLike],
  ) = {
    val (name, schema) = component
    schema match {
      case s: Schema if s.oneOf.nonEmpty =>
        val referencingSchemas = schemas.collect {
          case (parentName, parent: Schema) if parent.properties.exists {
                case (_, prop: Schema) => prop.$ref.contains(s"#/components/schemas/$name")
                case _ => false
              } =>
            parentName
        }
        referencingSchemas match {
          case single :: Nil => Some(single)
          case _ => None
        }
      case _ => None
    }
  }

  private def supplyComponents(
      component: asyncapi.Components,
      proto: ProtoInfo,
  ): asyncapi.Components = {
    val updatedSchemas: ListMap[String, Schema] =
      component.schemas.map(s => importSchema(s._1, s._2, proto))
    component.copy(schemas = updatedSchemas)
  }

  private def importSchemaLike(
      component: (String, SchemaLike),
      proto: ProtoInfo,
      parentComponent: Option[String],
  ): (String, SchemaLike) =
    component._2 match {
      case schema: Schema => importSchema(component._1, schema, proto, parentComponent)
      case _ => component
    }

  private def importSchema(
      componentName: String,
      componentSchema: Schema,
      proto: ProtoInfo,
      parentComponent: Option[String] = None,
  ): (String, Schema) =
    proto
      .findMessageInfo(componentName, parentComponent)
      .map { message =>
        val required = componentSchema.required.filter { fieldName =>
          message.isFieldRequired(fieldName)
        }

        val requiredFieldsWronglyAssumedOptional = componentSchema.properties.keys.filter {
          propertyName =>
            message.isFieldRequired(propertyName) &&
            !componentSchema.required.contains(propertyName)
        }
        val allRequired = (required ++ requiredFieldsWronglyAssumedOptional).toList.distinct

        val properties = componentSchema.properties.map { case (propertyName, propertySchema) =>
          message
            .getFieldComment(propertyName)
            .map { comments =>
              (
                propertyName,
                propertySchema match {
                  case pSchema: Schema =>
                    pSchema.copy(description = Some(comments))
                  case _ => propertySchema
                },
              )
            }
            .getOrElse(
              (propertyName, propertySchema)
            )
        }
        (
          componentName,
          componentSchema.copy(
            description = message.getComments(),
            properties = properties,
            required = allRequired,
          ),
        )
      }
      .getOrElse((componentName, componentSchema))

  def loadProtoData()(implicit traceContext: TraceContext): ProtoInfo =
    ProtoInfo
      .loadData()
      .fold(
        error => {
          logger.warn(s"Cannot load proto data for documentation $error")
          // If we cannot load protoInfo data then we  generate docs with no supplemented comments
          ProtoInfo(ExtractedProtoComments(SortedMap.empty, SortedMap.empty))
        },
        identity,
      )

  def createDocs(
      lapiVersion: String,
      endpointDescriptions: Seq[AnyEndpoint],
      protoData: ProtoInfo,
  ): ApiDocs = {
    val openApiDocs: openapi.OpenAPI = OpenAPIDocsInterpreter()
      .toOpenAPI(
        endpointDescriptions,
        "JSON Ledger API HTTP endpoints",
        lapiVersion,
      )
      .openapi("3.0.3")

    val cantonVersion = new Semver(lapiVersion).withClearedPreReleaseAndBuild().toString
    val supplementedOpenApi = supplyProtoDocs(openApiDocs, protoData, cantonVersion)
    import sttp.apispec.openapi.circe.yaml.*

    val asyncApiDocs: AsyncAPI = AsyncAPIInterpreter().toAsyncAPI(
      endpointDescriptions,
      "JSON Ledger API WebSocket endpoints",
      lapiVersion,
    )
    val supplementedAsyncApi = supplyProtoDocs(asyncApiDocs, protoData, cantonVersion)
    import sttp.apispec.asyncapi.circe.yaml.*

    val fixed3_0_3Api: OpenAPI = OpenAPI3_0_3Fix.fixTupleDefinition(supplementedOpenApi)

    val openApiYaml: String = fixed3_0_3Api.toYaml3_0_3
    val asyncApiYaml: String = supplementedAsyncApi.toYaml

    ApiDocs(openApiYaml, asyncApiYaml)
  }

  def createStaticDocs(protoInfo: ProtoInfo): ApiDocs =
    createDocs(
      VersionFile.readVersion().getOrElse("unknown"),
      staticDocumentationEndpoints.map(addHeaders),
      protoInfo,
    )

  private def addHeaders(endpoint: AnyEndpoint) =
    endpoint.in(headers)
}

final case class ApiDocs(openApi: String, asyncApi: String)

object OpenAPI3_0_3Fix {
  def fixTupleDefinition(existingDefinition: OpenAPI): OpenAPI = existingDefinition
    .modify(_.components.each.schemas.at("Tuple2_String_String"))
    .using {
      case schema: Schema =>
        schema.copy(
          prefixItems = None,
          items = Some(sttp.apispec.Schema(`type` = Some(List(apispec.SchemaType.String)))),
          minItems = Some(2),
          maxItems = Some(2),
        )
      case other => other
    }

}
