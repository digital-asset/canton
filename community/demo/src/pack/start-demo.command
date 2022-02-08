#!/bin/bash

cd "$(dirname "$0")" || exit 1

java javafx.scene.control.TableCell 2>&1 | grep -q "Main method not found"
NATIVE_SUPPORT=$?

JAVA_VERSION=$(java -version 2>&1 | grep version | awk '{print $3}')

JAVA_MAJOR_VERSION=$(sed -E 's/"(1\.)?([0-9]+)\..*/\2/' <<< "$JAVA_VERSION")

echo "Detected Java version $JAVA_VERSION (major: $JAVA_MAJOR_VERSION)."

if [ "$JAVA_MAJOR_VERSION" -lt 11 ]; then
  echo "Please use Java version 11 or higher."
  exit 1
else
  if [ $NATIVE_SUPPORT -ne 0 ]; then
    echo "Java FX is not installed. It will be fetched automatically."
  else
    echo "Looks like your JRE has native support for JavaFX"
  fi
fi


bin/canton -v -c demo/demo.conf --bootstrap demo/demo.sc --no-tty
# Run with --no-tty, as the demo otherwise tends to mess up terminal settings,
# e.g., input feedback and line breaks are gone after running the demo.
