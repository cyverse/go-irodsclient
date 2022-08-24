#! /bin/bash
cfg=config.inc

set -o errexit -o nounset -o pipefail

if [[ "$OSTYPE" == "darwin"* ]]
then
  readonly ExecName=$(greadlink -f "$0")
else
  readonly ExecName=$(readlink --canonicalize "$0")
fi

main()
{
  local baseDir=$(dirname "$ExecName")

  if [ -z "$cfg" ]
  then
    printf 'An environment variable include file is needed.\n' >&2
    return 1
  fi

  . "$baseDir/$cfg"

  docker-compose --file "$baseDir"/docker-compose.yml --project-name "$ENV_NAME" down --remove-orphans
}


main "$@"
