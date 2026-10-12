{ pkgs ? import <nixpkgs> {} }:

let
  sources = builtins.fromJSON (builtins.readFile ./sources.json);
  system = pkgs.stdenv.hostPlatform.system;
  platform = sources.platforms.${system} or (throw "dpm: unsupported system ${system}");
in
pkgs.stdenv.mkDerivation {
  pname = "dpm";
  inherit (sources) version;

  src = pkgs.fetchurl {
    url = "https://github.com/digital-asset/dpm/releases/download/${sources.version}/dpm-${sources.version}-${platform.oci}.tar.gz";
    inherit (platform) sha256;
  };

  sourceRoot = ".";
  dontConfigure = true;
  dontBuild = true;

  installPhase = ''    runHook preInstall
    mkdir -p $out/bin
    cp -r * $out/bin
    runHook postInstall
  '';
}
