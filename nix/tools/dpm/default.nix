{ pkgs ? import <nixpkgs> {} }:

let
  dpmPath = "europe-docker.pkg.dev/da-images/public/components/dpm";
  dpmVersion = "1.0.13";
  dpmRef = "${dpmPath}:${dpmVersion}";

  # These are Nix recursive output hashes for the directory produced by
  # `oras pull -o $out` for each platform-specific artifact.
  dpmHashes = {
    "x86_64-linux" = "sha256-8w3bGxFXoAy6d2YykgBtsMX4ZhuO/HlGf7jWc9pTJ6I=";
    "aarch64-linux" = "sha256-djrUlN693+uV7PeeLxrtkNI2bxAsvLYmtJEkGRlwd5I=";
    "x86_64-darwin" = "sha256-lNfLYRZnIWBu1V164PADety6ERD4g9U3fG1uT9lqNyo=";
    "aarch64-darwin" = "sha256-ELr+LGE8i1Z7QRylqtFc7W/b8qj7jFBATneIyqlxvhs=";
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
