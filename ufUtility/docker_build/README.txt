Docker file to convert .a18 files.

AudioBatchConverter     32-bit linux application to convert .a18 -> .wav and .wav -> .a18.
conv.sh                 Shell script that runs in Docker container. Invokes AudioBatchConverter 
                        and ffmpeg to convert between .a18 and any format that ffmpeg supports.
go.sh                   A sample "escape" file that can be used to run arbitrary conversion 
                        from the Docker script. Look at conv.sh to see how it is invoked, and 
                        study the sample for ideas.
dockerfile              Defines the container.

Usage:
docker run  --rm \
            --platform linux/386  \
            --mount 'type=bind,source=/Users/bill/A-sounds/audio/.,target=/audio' \
            --mount 'type=bind,source=/Users/bill/A-sounds/out/.,target=/out' \
            'amplionetwork/ac:1.0' \
            1kHz.wav \
            /out/1kHz.a18

Where:
    --rm        Clean up after running
    --platform  On Macos, tells docker to run a 32-bit vm
    --mount     Absolute path to where the input file will be
    --mount     Absolute path to where the output file can go
    amplionetwork/ac:1.0    The container to run
    1kHz.wav    Input file, in the 'audio' mount
    /out/1kHz.a18   Output file, here placed into the 'out' mount  

Alternatively:
docker run  --rm \
            --platform linux/386  \
            --mount 'type=bind,source=/Users/bill/A-sounds/audio/.,target=/audio'  \
            'amplionetwork/ac:1.0' \
            1kHz.wav \
            /audio/1kHz.a18

Same, except the output goes next to the input.              