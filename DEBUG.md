# Debugging proxyfsd

## Logging
Logs from proxyfsd are output to `proxyfsd.log`.

## Kill with trace data
Find the daemon's pid: `ps -eaf | grep proxyfsd`

Kill the process and get golang stack traces: `kill -ABRT <pid>`

## Using gdb
Find the daemon's pid: `ps -eaf | grep proxyfsd`

Tell gdb to attach to that pid: `gdb -p <pid>`

## Generating a core file
If you want to generate a core file from within gdb: `(gdb) gcore`

If you want to generate a core file without using gdb: `gcore <pid>`
