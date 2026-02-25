{ pkgs ? import <nixpkgs> {} }:

let
  dpmPath = "europe-docker.pkg.dev/da-images/public/components/dpm";
  dpmVersion = "1.0.10";
  dpmRef = "${dpmPath}:${dpmVersion}";

  dpmHashes = {
    "x86_64-linux" = "sha256-5f/NkVOG40lZLgQvtcclSTzqeqN2UMzSho9tSX4AhUc=";
    "aarch64-linux" = "sha256-R66Ns/gILU12SkBy87KuhudI4kfI9EHrjeLBWRmXV90=";
    "x86_64-darwin" = "sha256-SDdNgrw/E+jFSSoOHt5Sm2ZNo/X97BbISHhRtfNtdUw=";
    "aarch64-darwin" = "sha256-q2DthxO0qHzwSlc7G39je5nfe2x3+yh2freSZrMmMAE=";
  };
  dpmHash = dpmHashes.${pkgs.system} or (throw "Unsupported system: ${pkgs.system}");

  ociPlatforms = {
    "x86_64-linux" = "linux/amd64";
    "aarch64-linux" = "linux/arm64";
    "x86_64-darwin" = "darwin/amd64";
    "aarch64-darwin" = "darwin/arm64";
  };
  ociPlatform = ociPlatforms.${pkgs.system} or (throw "Unsupported system: ${pkgs.system}");
in
pkgs.stdenv.mkDerivation {
  pname = "dpm";
  version = dpmVersion;

  src = pkgs.stdenv.mkDerivation {
    name = "dpm-pull-${dpmRef}.${ociPlatform}";

    nativeBuildInputs = [ pkgs.oras pkgs.cacert ];

    buildCommand = ''
      set -e
      echo "Pulling dpm component from ${dpmRef}.${ociPlatform}"
      oras pull --platform ${ociPlatform} -o $out ${dpmRef}
    '';
    outputHashMode = "recursive";
    outputHashAlgo = "sha256";
    outputHash = dpmHash;
  };

  installPhase = ''
    mkdir -p $out/bin
    install -Dm755 $src/dpm $out/bin/dpm
  '';
}
