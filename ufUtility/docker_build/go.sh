set -x
echo Arguments are ${*}

shift
echo Arguments now ${*}

from="${1}"
fromExt=${from##*.}
to="${2}"
toExt=${to##*.}

echo "Convert from ${from} to ${to} as ${fromExt} => ${toExt}"

if [ "$fromExt" == "a18" ] || [ "$fromExt" ==  "A18" ]; then
    echo convert from .a18
    a18out="/tmp/${from}.wav"
    /app/AudioBatchConverter -d ${from} -o /tmp
    # If the desired output is .wav, just move the file.
    if [ "$toExt" == "wav" ] || [ "$toExt" == "WAV" ]; then
        mv ${a18out} ${to}
    else
        ffmpeg -i ${a18out} -y ${to}
    fi
elif [ "$toExt" == "a18" ] || [ "${toExt}" == "A18" ]; then
    echo convert to .a18
    # If the source file is already .wav, just use it.
    # if [ "$toExt" == "wav" ] || [ "$toExt" == "WAV" ]; then
    #     a18in=${from}
    # else
        a18in="/tmp/${from}.wav"
        ffmpeg -i ${from} -ab 16k -ar 16000 -ac 1 -y ${a18in}
    # fi
    a18out="${a18in}.a18"
    /app/AudioBatchConverter -e a1800 -b 16000 -o /tmp ${a18in}
    mv ${a18out} ${to}
fi
