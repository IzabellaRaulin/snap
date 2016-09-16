#!/bin/bash
snapctl plugin load ../build/plugin/snap-plugin-collector-mock1
snapctl plugin load ../build/plugin/snap-plugin-publisher-mock-file
snapctl plugin load ../build/plugin/snap-plugin-processor-passthru

#snapctl task create -t  mock-file_specific_instance.json
#snapctl task create -t  mock-file_specific_instance_tuple.json
#snapctl task create -t mock-file_query.json
#snapctl task create -t mock-file_query2.json
#snapctl plugin load ../build/plugin/snap-collector-mock2
#snapctl plugin load /home/test/Pulse_telemetry/pulse/src/github.com/intelsdi-x/snap-plugin-collector-users/build/rootfs/snap-plugin-collector-users

