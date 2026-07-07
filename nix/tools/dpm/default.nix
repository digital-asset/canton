{ pkgs ? import <nixpkgs> {} }:

let
  dpmPath = "ghcr.io/digital-asset/temp/components/dpm";
  dpmVersion = "1.0.20";
  dpmRef = "${dpmPath}:${dpmVersion}";

  # These are OCI layer digests for each platform-specific artifact.
  dpmHashes = {
    "x86_64-linux" = "sha256-Nzg47tUPIgqRCAQgBVRgXDo+Kiz16kHL3bhMH6uT374=";
    "aarch64-linux" = "sha256-Dd7v8/8nst5tTL8bGoxAcIMWL2EIxBlrv9zESKqeXXU=";
    "x86_64-darwin" = "sha256-FHLLSsQroWH8PhE/kMN5fch+OfmPDy5jxpt8PG4pvs0=";
    "aarch64-darwin" = "sha256-izl9SStIMyI0cjrx8Qv2VYkSLvdjPUjRvfNayhsqM9s=";
  };
  dpmHash = dpmHashes.${pkgs.stdenv.hostPlatform.system} or (throw "Unsupported system: $pkgs.stdenv.hostPlatform.system}");

  ociPlatforms = {
    "x86_64-linux" = "linux/amd64";
    "aarch64-linux" = "linux/arm64";
    "x86_64-darwin" = "darwin/amd64";
    "aarch64-darwin" = "darwin/arm64";
  };
  ociPlatform = ociPlatforms.${pkgs.stdenv.hostPlatform.system} or (throw "Unsupported system: ${pkgs.stdenv.hostPlatform.system}");
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
