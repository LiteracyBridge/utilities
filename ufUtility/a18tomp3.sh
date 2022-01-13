#!/usr/bin/env zsh
#
# Sample shell script showing conversion from .a18 to .mp3
#

set -x

audio=(--mount type=bind,source="$(pwd)/.",target=/audio)
out=(--mount type=bind,source="$(pwd)/tmp",target=/out)
# specify platform because we're running a 386 image, but probably running on x86_64
cmd=(docker run --rm --platform linux/386)
container="amplionetwork/abc:1.0"


# docker run --rm --platform linux/386 --mount type=bind,source="$(pwd)/.",target=/audio --mount type=bind,source="$(pwd)/foo",target=/out dockerfile 

mkdir -p tmp
${cmd} ${audio} ${out} ${container} -o /out $1.a18 
ffmpeg -v 0 -y -i tmp/$1.a18.wav $1.mp3
