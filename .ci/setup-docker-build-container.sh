#!/usr/bin/env bash
set -eo pipefail
# configure nix
mkdir -p "${HOME}/.config/nix"
# disable sandbox, since it doesn't work on CircleCI (sethostname is not allowed)
cat > "${HOME}/.config/nix/nix.conf" <<'EOF'
sandbox = false
bash-prompt-prefix = (nix:$name)\040
auto-optimise-store = true
experimental-features = nix-command flakes
EOF
# configure direnv to auto allow /home/circleci/project
mkdir -p "${HOME}/.config/direnv"
cat > "${HOME}/.config/direnv/direnv.toml" <<'EOF'
[whitelist]
prefix = [ "/home/circleci/project" ]
EOF
# provision nix packages
cd /tmp && \
  nix-shell -I nixpkgs=./nix/nixpkgs.nix shell.nix --run 'echo "Done loading all packages."'
shell_nix_size="$(du -hs /nix | cut -f1)"
# install direnv as single binary without any deps
curl -sfL https://direnv.net/install.sh | bash
# link profile bin to /bin, for more capability like /bin/sh /bin/bash, etc.
for i in 'bin' 'sbin'; do
 if [[ -d "${HOME}/.nix-profile/${i}" ]]; then
  for item in $(ls ${HOME}/.nix-profile/${i}/*); do
    sudo ln -svf "$(realpath ${item})" "/${i}/$(basename ${item})"
  done
 fi
done
cd /tmp && \
  nix-shell -I nixpkgs=./nix/nixpkgs.nix shell.nix --run 'for item in "docker-credential-gcloud" "gcloud" "python3" "lnav" "aws" "jo" "jq" "gh" "rg" "hub" "amm" "buf" "pigz" "circleci" "azcopy"; do\
    if item_path=$(which $item); then ln -svf "$(which $item)" "${HOME}/bin/${item}"; fi; done'
# allow git folders
cd ${HOME} && \
  git config --global --add safe.directory "*" || exit 1
# summary size stat
echo "Size /nix shell.nix: ${shell_nix_size}"
echo "  Size ~/.cache/nix: $(du -sh ~/.cache/nix | cut -f1)"
# clean up nix cache
rm -rf ~/.cache/nix
