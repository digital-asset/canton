// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.logging.ErrorLoggingContext
import com.typesafe.config.{ConfigFactory, ConfigUtil}
import pureconfig.{ConfigCursor, ConfigReader, PathSegment}

import scala.jdk.CollectionConverters.*

object DeprecatedConfigUtils {
  case class MovedConfigPath(from: String, to: String)

  object DeprecatedFieldsFor {
    def combine[T](instances: DeprecatedFieldsFor[_ >: T]*): DeprecatedFieldsFor[T] =
      new DeprecatedFieldsFor[T] {
        override def movedFields: List[MovedConfigPath] = instances.flatMap(_.movedFields).toList
      }
  }

  /** Simple typeclass to provide deprecated fields for a config class
    * @tparam T type of the config being targeted
    */
  trait DeprecatedFieldsFor[-T] {
    def movedFields: List[MovedConfigPath]
  }

  implicit class EnhancedConfigReader[T](val configReader: ConfigReader[T]) extends AnyVal {

    /** Moves the value at 'from' to 'to' if 'to' is not already set, and removes 'from' from the config.
      * @param from path to the deprecated field. e.g: "path.to.deprecated.field"
      * @param to string path to the new field. e.g: "path.to.new.field"
      * @return config reader with fallback values from the deprecated fields
      */
    def moveDeprecatedField(from: String, to: String)(implicit
        elc: ErrorLoggingContext
    ): ConfigReader[T] = {
      val fromPathSegment =
        ConfigUtil.splitPath(from).asScala.toList.map(PathSegment.stringToPathSegment)
      configReader
        // Modify the config before attempting to parse it to the final scala type
        .contramapCursor { cursor =>
          val result = for {
            // Get current config value
            cursorConfigValue <- cursor.asConfigValue
            // Get the config value at "from" (deprecated path we want to move)
            cursorAtFrom <- cursor.fluent.at(fromPathSegment: _*).cursor
            fromValueOpt = cursorAtFrom.valueOpt
            adjustedConfig = fromValueOpt
              .map { deprecated =>
                // Log a message if it turns out there's a value defined at the deprecated path
                elc.info(
                  s"Config field at $from is deprecated. Please use $to instead."
                )
                // Build a new config from scratch by
                ConfigFactory
                  .empty()
                  // Adding back all the values from the original config
                  .withFallback(cursorConfigValue)
                  // Adding the deprecated value to its new location
                  .withFallback(deprecated.atPath(to))
                  // Deleting the deprecated value from the config, so that we don't get an "Unkown key" error later
                  .withoutPath(from)
                  .root()
              }
              .getOrElse(cursorConfigValue)
          } yield ConfigCursor(adjustedConfig, cursor.pathElems)

          result.getOrElse(cursor)
        }
    }

    /** Applies a list of deprecation fallbacks to the configReader
      * @return config reader with fallbacks applied
      */
    def applyDeprecations(implicit
        elc: ErrorLoggingContext,
        deprecatedFieldsFor: DeprecatedFieldsFor[T],
    ): ConfigReader[T] = {
      implicitly[DeprecatedFieldsFor[T]].movedFields
        .foldLeft(configReader) { case (reader, field) =>
          reader.moveDeprecatedField(field.from, field.to)
        }
    }
  }
}
