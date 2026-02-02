#!/usr/bin/env bash
ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
# shellcheck source=./common.sh
source "$ABSDIR/common.sh"

getchar() {
	read -rN1 ch
	[[ -z $ch ]] && exit
}

putchar() {
	echo -n "$ch"
}

# needs to be global, otherwise \n is treated as empty for some reason :(
ch=initial
while [[ -n $ch ]]; do
	getchar

	while [[ $ch == $'\r' ]]; do
		getchar
		[[ $ch != $'\n' ]] && putchar
	done

	if [[ $ch == $'\x1b' ]]; then
		getchar
		case "$ch" in
		"[")
			getchar
			while [[ $ch =~ ['0-9;?'] ]]; do
				getchar
			done
			;;
		"]")
			getchar
			if [[ $ch =~ [0-9] ]]; then
				while true; do
					getchar
					if [[ $ch == $'\x7' ]]; then
						break
					elif [[ $ch == $'\x1b' ]]; then
						getchar
						break
					fi
				done
			fi
			;;
		"%")
			getchar
			;;
		*) ;;
		esac
	else
		putchar
	fi
done
