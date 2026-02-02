# Update nixpkgs with:
# nix-shell -p niv --run "niv update"
# NOTE: After the update build a new docker image for CI

let
  sources = import ./nix/sources.nix;
  pkgs = import sources.nixpkgs { };
  jre = pkgs.openjdk21_headless;
  dpm = pkgs.callPackage ./nix/tools/dpm {};
in
pkgs.mkShell {
  packages = with pkgs; [
    (ammonite_2_13.override { inherit jre; })
    awscli
    azure-storage-azcopy
    bashInteractive
    buf
    circleci-cli
    curl
    dpm
    entr
    (flyway.override { jre_headless = jre; })
    gitAndTools.gh
    gitAndTools.hub
    gitMinimal
    glibcLocales
    gnugrep
    gnupg
    go
    go-jsonnet
    google-cloud-sdk
    haproxy
    jo
    jq
    jre
    jsonnet-bundler
    lnav
    locale
    nodejs
    openssl
    oras
    pigz
    postgresql_17
    (python3.withPackages (pkgs: [ pkgs.datadog pkgs.sphinx pkgs.sphinx_rtd_theme pkgs.sphinx-togglebutton pkgs.sphinx-copybutton pkgs.sphinx-tabs pkgs.pip pkgs.setuptools pkgs.cryptography pkgs.grpcio-tools pkgs.protobuf pkgs.pandas pkgs.dash pkgs.plotly ]))
    ripgrep
    toxiproxy
    unzip
    (sbt.override { inherit jre; })
    xxd
    zip
    tinyproxy

    # Vale with styles
    (vale.withStyles (styles: [ styles.google ]))
  ];

  # needed by curl
  SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
  # Avoid sbt-assembly falling over. See https://github.com/sbt/sbt-assembly/issues/496
  LANG = "en_US.UTF-8";
  LC_ALL = "en_US.UTF-8";
  # Avoid "warning: setlocale: LC_ALL: cannot change locale (C.UTF-8)"
  # warnings in damlc.
  LOCALE_ARCHIVE = if pkgs.stdenv.hostPlatform.libc == "glibc"
                        then "${pkgs.glibcLocales}/lib/locale/locale-archive"
                        else null;
  shellHook = ''
    # - nix-shell sets the environment variable SOURCE_DATE_EPOCH to a value corresponding to the year 1980
    # - when SOURCE_DATE_EPOCH is set, then Sphinx "fixes" automagically the given year in the copyright statement in the
    #   'build configuration file' (conf.py) with that value, e.g. instead of 'Copyright 2023, Digital Asset.' we get
    #   'Copyright 1980, Digital Asset.' in the resulting documentation
    unset SOURCE_DATE_EPOCH
  '';
}

