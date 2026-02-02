Nix
===

We use [NixOS](https://nixos.org/) to configure the shell used for development including development tools such as
the JDK, python, grep, ...
This guides explains how to change the Nix image.

# Updating nixpkgs

All dependency versions are pinned to a specific commit of the nixpkgs repository (github.com/NixOS/nixpkgs) using [niv](https://github.com/nmattia/niv).

In order to update the nixpkgs source, run `nix-shell -p niv --run "niv update"` and commit the changed `nix/sources.{nix,json}` files.

_Note:_ after changing the nixpkgs sources, run the manual job `update_canton_build_docker_image` in CI.

# Adding new packages

You can search the nixpkgs repository here: https://search.nixos.org/packages

If you want to make a dev tool available in the nix shell including manpages and command line completions, add the package
to the `packages` list, otherwise to the `buildInputs` list in the `shell.nix` file.
