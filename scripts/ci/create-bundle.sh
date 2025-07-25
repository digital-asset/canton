#!/bin/bash

function adjust_startup_scripts() {
  TMP=$(echo $JAR | sed -E 's/.*canton-([^-]+)-.*/\1/')
  TMP="$(tr '[:lower:]' '[:upper:]' <<< ${TMP:0:1})${TMP:1}"
  if [ -z "${TMP}" ]; then
    echo "ERROR, failed to extract the release type from the JAR name"
    exit 1
  fi
  REPLACE_PROG_NAME="\"Canton ${TMP}\""
  REPLACE_VERSION=$(cut -d\" -f4 < version.sbt)
  REPLACE_REVISION=$(git rev-parse HEAD)
  REPLACE_JVM_OPTS="-XX:+CrashOnOutOfMemoryError"
  REPLACE_JAR="lib\/$RELEASE.jar"
  REPLACE_MAIN_CLASS="$MAIN_CLASS"
  REPLACE_MAC_ICON_FILE="lib\/canton.ico"
  for ff in "bin/canton" "bin/canton.bat"
  do
      cat $RELEASE_DIR/$ff |
        sed -e "s/REPLACE_PROG_NAME/${REPLACE_PROG_NAME}/" |
        sed -e "s/REPLACE_VERSION/${REPLACE_VERSION}/" |
        sed -e "s/REPLACE_REVISION/${REPLACE_REVISION}/" |
        sed -e "s/REPLACE_JVM_OPTS/${REPLACE_JVM_OPTS}/" |
        sed -e "s/REPLACE_JAR/${REPLACE_JAR}/" |
        sed -e "s/REPLACE_MAIN_CLASS/${REPLACE_MAIN_CLASS}/" |
        sed -e "s/REPLACE_MAC_ICON_FILE/${REPLACE_MAC_ICON_FILE}/" > $RELEASE_DIR/tmp.txt
      mv $RELEASE_DIR/tmp.txt $RELEASE_DIR/$ff
      chmod 755 $RELEASE_DIR/$ff
  done
}

function get_release_name() {
  if [ -z "${RELEASE_SUFFIX}" ]; then
    RELEASE=$(echo $JAR | sed -e 's/\.jar$//')
    echo "$RELEASE"
  else
    TMP=$(echo $JAR | grep -Eo "canton-([^[:digit:]]+)")
    RELEASE="$TMP$RELEASE_SUFFIX"
    echo "$RELEASE"
  fi
}

set -e

JARFILE=$1
shift
MAIN_CLASS=$1
shift
PACKS=$@

JAR=$(basename $JARFILE)
RELEASE=$(get_release_name)
echo $RELEASE

TARGET=$(dirname $JARFILE)/../release
RELEASE_DIR=$TARGET/$RELEASE

rm -rf $RELEASE_DIR
mkdir -p $RELEASE_DIR/lib

echo "assembling release in $RELEASE_DIR"

cp -v $JARFILE $RELEASE_DIR/lib/$RELEASE.jar

state="scan"

for ff in $PACKS
do
  case $state in
    "scan")
      case $ff in
        "-c")
          state="copy"
          ;;
        "-l")
          state="copyLinks"
          ;;
        "-r")
          state="rename"
          ;;
        *)
          echo "ERROR, expected -r or -c, found ${ff}"
          exit 1
      esac
      ;;
    "copy")
      if [[ -e $ff ]]; then
        if [[ -d $ff ]]; then
          if [[ -z $(ls -A $ff) ]]; then
            echo "skipping empty $ff"
          else
            echo "copying content from $ff"
            cp -r $ff/* $RELEASE_DIR
          fi
        else
          echo "copying file $ff"
          cp $ff $RELEASE_DIR
        fi
      else
        echo "ERROR, no such file $ff for copying"
        exit 1
      fi
      state="scan"
      ;;
    "copyLinks")
          if [[ -e $ff ]]; then
            if [[ -d $ff ]]; then
              if [[ -z $(ls -A $ff) ]]; then
                echo "skipping empty $ff"
              else
                echo "copying content from $ff"
                cp -rL $ff/* $RELEASE_DIR
              fi
            else
              echo "copying file $ff"
              cp -L $ff $RELEASE_DIR
            fi
          else
            echo "ERROR, no such file $ff for copying"
            exit 1
          fi
          state="scan"
          ;;
    "rename")
      if [[ -e $ff ]]; then
        rename=$ff
      else
        echo "ERROR, no such file $ff for renaming"
        exit 1
      fi
      state="rename-do"
      ;;
    "rename-do")
      target=$RELEASE_DIR/$ff
      target_dir=$(dirname $target)
      if [[ ! -e $target_dir ]]; then
        mkdir -p $target_dir
      fi
      cp -v $rename $target
      state="scan"
      ;;
    *)
      echo "unexpected state $state"
      exit 1
  esac
done

adjust_startup_scripts

cd $TARGET

# pack releases
rm -f "${RELEASE}.tar.gz"
rm -f "${RELEASE}.zip"
tar -zcf "${RELEASE}.tar.gz" $RELEASE &
zip -rq "${RELEASE}.zip" $RELEASE/* &
tar -zcf "${RELEASE}-protobuf.tar.gz" $RELEASE/protobuf &
zip -rq "${RELEASE}-protobuf.zip" $RELEASE/protobuf/* &
wait

# finally, add a stable link to the directory
rm -f canton
ln -s $RELEASE canton
