// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.digitalasset.canton.http.json.JsHealthService
import com.digitalasset.canton.http.json.v2.JsSchema.X_ONE_OF
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.services.VersionFile
import com.softwaremill.quicklens.*
import monocle.macros.syntax.lens.*
import org.semver4j.Semver
import sttp.apispec
import sttp.apispec.asyncapi.{AsyncAPI, ChannelItem, ReferenceOr}
import sttp.apispec.openapi.{OpenAPI, Operation, PathItem}
import sttp.apispec.{Schema, SchemaLike, asyncapi, openapi}
import sttp.tapir.AnyEndpoint
import sttp.tapir.docs.asyncapi.AsyncAPIInterpreter
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter

import scala.collection.immutable.ListMap

class ApiDocsGenerator(override protected val loggerFactory: NamedLoggerFactory)
    extends NamedLogging {

  private val releaseNote = """
    |This specification version fixes the API inconsistencies where certain fields marked as required in the spec are in fact optional.
    |If you use code generation tool based on this file, you might need to adjust the existing application code to handle those fields as optional.
    |If you do not want to change your client code, continue using the OpenAPI specification for the latest Canton 3.4 patch release.
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
        JsHealthService,
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
    val updatedPaths =
      initial.paths.pathItems.map { case (path, pathItem) =>
        (path, supplyPathItem(pathItem, proto))
      }
    initial
      .focus(_.components)
      .replace(updatedComponents)
      .focus(_.paths.pathItems)
      .replace(updatedPaths)
      .focus(_.info.description)
      .replace(Some(s"$releaseNote\nMINIMUM_CANTON_VERSION=$minimalCantonVersion"))
  }
  private def supplyPathItem(pathItem: PathItem, proto: ProtoInfo) =
    pathItem.copy(
      get = pathItem.get.map(withSuppliedServiceDescription(_, proto)),
      put = pathItem.put.map(withSuppliedServiceDescription(_, proto)),
      post = pathItem.post.map(withSuppliedServiceDescription(_, proto)),
      delete = pathItem.delete.map(withSuppliedServiceDescription(_, proto)),
      options = pathItem.options.map(withSuppliedServiceDescription(_, proto)),
      head = pathItem.head.map(withSuppliedServiceDescription(_, proto)),
      patch = pathItem.patch.map(withSuppliedServiceDescription(_, proto)),
      trace = pathItem.trace.map(withSuppliedServiceDescription(_, proto)),
    )

  private def supplyServiceDescriptions(description: String, proto: ProtoInfo) = {
    val lines: Seq[String] = description
      .split("\n")
      .toSeq
      .flatMap { (line: String) =>
        line match {
          case ProtoLink(protoFile, serviceName, methodName) =>
            Seq(proto.findServiceDescription(protoFile, serviceName, methodName))
          case other => Seq(other)
        }
      }
    lines.mkString("\n")
  }

  private def withSuppliedServiceDescription(operation: Operation, proto: ProtoInfo) =
    operation.focus(_.description).some.modify(supplyServiceDescriptions(_, proto))

  private def supplyProtoDocs(
      initial: asyncapi.AsyncAPI,
      proto: ProtoInfo,
      minimalCantonVersion: String,
  ): asyncapi.AsyncAPI = {
    val updatedComponents = initial.components.map(component => supplyComponents(component, proto))
    val updateChannels = initial.channels.map { case (channelName, channel) =>
      (channelName, updateChannel(channel, proto))
    }
    initial
      .focus(_.components)
      .replace(updatedComponents)
      .focus(_.channels)
      .replace(updateChannels)
      .focus(_.info.description)
      .replace(Some(s"$releaseNote\nMINIMUM_CANTON_VERSION=$minimalCantonVersion"))
  }

  private def updateChannel(
      channel: ReferenceOr[ChannelItem],
      proto: ProtoInfo,
  ): ReferenceOr[ChannelItem] = {
    def fixOpDescription(operation: Option[asyncapi.Operation], ch: ChannelItem) = operation.map {
      op =>
        op.copy(
          description = if (op.description == ch.description) {
            // Remove redundant descriptions
            None
          } else {
            op.description.map(description => supplyServiceDescriptions(description, proto))
          }
        )
    }
    channel.map { ch =>
      val subscribe = fixOpDescription(ch.subscribe, ch)
      val publish = fixOpDescription(ch.publish, ch)

      // format: off
      ch.focus(_.description).some.modify(supplyServiceDescriptions(_, proto))
        .focus(_.subscribe).replace(subscribe)
        .focus(_.publish).replace(publish)
      // format: on
    }
  }

  private def supplyComponents(
      component: openapi.Components,
      proto: ProtoInfo,
  ): openapi.Components = {
    val schemasInOneOfWithExtension = findSchemasInOneOfWithExtension(component.schemas)

    val updatedSchemas: ListMap[String, SchemaLike] =
      component.schemas.map(s =>
        importSchemaLike(
          component = s,
          proto = proto,
          parentComponent = findUniqueParent(s, component.schemas),
          schemasInOneOfWithExtension = schemasInOneOfWithExtension,
        )
      )

    val schemasWithoutXOneOf: ListMap[String, SchemaLike] = updatedSchemas.map {
      case (name, schema: Schema) => (name, removeXOneOfExtension(schema))
      case other => other
    }

    component.copy(schemas = schemasWithoutXOneOf)
  }

  private def findSchemasInOneOfWithExtension(schemas: ListMap[String, SchemaLike]): Set[String] =
    schemas
      .collect {
        case (_, parent: Schema) if parent.oneOf.nonEmpty && parent.extensions.exists {
              case (key, value) =>
                key == X_ONE_OF && value.toString.contains("true")
            } =>
          parent.oneOf.flatMap {
            case variantSchema: Schema =>
              variantSchema.properties.values.collect {
                case propSchema: Schema if propSchema.$ref.isDefined =>
                  propSchema.$ref.fold("")(_.split("/").lastOption.getOrElse(""))
              }
            case _ => Nil
          }
      }
      .flatten
      .toSet

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
    val schemasInOneOfWithExtension = findSchemasInOneOfWithExtension(component.schemas)

    val updatedSchemas: ListMap[String, Schema] =
      component.schemas.map(s => importSchema(s._1, s._2, proto, None, schemasInOneOfWithExtension))

    val schemasWithoutXOneOf: ListMap[String, Schema] = updatedSchemas.map { case (name, schema) =>
      (name, removeXOneOfExtension(schema))
    }

    component.copy(schemas = schemasWithoutXOneOf)
  }

  private def importSchemaLike(
      component: (String, SchemaLike),
      proto: ProtoInfo,
      parentComponent: Option[String],
      schemasInOneOfWithExtension: Set[String],
  ): (String, SchemaLike) =
    component._2 match {
      case schema: Schema =>
        importSchema(component._1, schema, proto, parentComponent, schemasInOneOfWithExtension)
      case _ => component
    }

  private def importSchema(
      componentName: String,
      componentSchema: Schema,
      proto: ProtoInfo,
      parentComponent: Option[String],
      schemasInOneOfWithExtension: Set[String],
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
                  case pSchema: Schema => pSchema.copy(description = Some(comments))
                  case _ => propertySchema
                },
              )
            }
            .getOrElse((propertyName, propertySchema))
        }

        val finalRequired =
          if (
            shouldMakeValueRequired(
              componentSchema,
              message,
              allRequired,
              componentName,
              schemasInOneOfWithExtension,
            )
          ) {
            (allRequired :+ "value").distinct
          } else {
            allRequired
          }

        (
          componentName,
          componentSchema.copy(
            description = message.getComments(),
            properties = properties,
            required = finalRequired,
          ),
        )
      }
      .getOrElse((componentName, componentSchema))

  private def shouldMakeValueRequired(
      schema: Schema,
      message: MessageInfo,
      currentRequired: List[String],
      componentName: String,
      schemasInOneOfWithExtension: Set[String],
  ): Boolean = {
    val isInOneOfWithExtension = schemasInOneOfWithExtension.contains(componentName)
    val hasSingleValueProperty =
      schema.properties.sizeIs == 1 && schema.properties.contains("value")
    val valueNotAlreadyRequired = !currentRequired.contains("value")
    val protoDoesntSpecifyValue =
      !message.isFieldRequired("value") && !message.isFieldOptional("value")

    isInOneOfWithExtension && hasSingleValueProperty && valueNotAlreadyRequired && protoDoesntSpecifyValue
  }

  private def removeXOneOfExtension(schema: Schema): Schema = {
    val cleanedExtensions = schema.extensions.filterNot { case (key, _) => key == X_ONE_OF }

    val cleanedProperties = schema.properties.map {
      case (propName, propSchema: Schema) => (propName, removeXOneOfExtension(propSchema))
      case other => other
    }

    val cleanedOneOf = schema.oneOf.map {
      case variantSchema: Schema => removeXOneOfExtension(variantSchema)
      case other => other
    }

    schema.copy(
      extensions = cleanedExtensions,
      properties = cleanedProperties,
      oneOf = cleanedOneOf,
    )
  }

  def loadProtoData(): ProtoInfo =
    ProtoInfo
      .loadData()

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
      staticDocumentationEndpoints,
      protoInfo,
    )

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
