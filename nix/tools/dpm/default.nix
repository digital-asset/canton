{ pkgs ? import <nixpkgs> {} }:

let
  dpmVersion = "1.0.22";

  dpmHashes = {
    "x86_64-linux" = sha256:1zw8w0vgjfz1m2fpk22dchvdavss38i9n7ycyjab1r325q3v5zgb;
    "aarch64-linux" = sha256:1sbp4smsffgm984fib4p6g4z1mgbd5652ywr1rhx9hmc2kq8lb1n;
    "x86_64-darwin" = "sha256:0ill45s9zgxpgr7x0p16ahbf43sxx7136c0vi88msxl7ciyziljj";
    "aarch64-darwin" = "sha256:1nq2m141nvmis1axjl1v290q17lxcmxnqv6ssmcha3p1aa2im1z6";
  };
  dpmHash = dpmHashes.${pkgs.stdenv.hostPlatform.system} or (throw "Unsupported system: ${pkgs.stdenv.hostPlatform.system}");

  ociPlatforms = {
    "x86_64-linux" = "linux-amd64";
    "aarch64-linux" = "linux-arm64";
    "x86_64-darwin" = "darwin-amd64";
    "aarch64-darwin" = "darwin-arm64";
  };
  ociPlatform = ociPlatforms.${pkgs.stdenv.hostPlatform.system} or (throw "Unsupported system: ${pkgs.stdenv.hostPlatform.system}");
in
pkgs.stdenv.mkDerivation {
  name = "dpm-gh";

 src = builtins.fetchurl {
    url = "https://github.com/digital-asset/dpm/releases/download/${dpmVersion}/dpm-${dpmVersion}-${ociPlatform}.tar.gz";
    sha256 = "${dpmHash}";
  };

  sourceRoot = ".";
  installPhase = ''
    mkdir -p $out/bin
    cp -r * "$out/bin"
  '';
}
