# How to use SCDBClient

The SCDBClient is a terminal client that allows users to work with the SCDB server. The client provides several commands and options for interacting with the server.

## Build

build scdbclient from source

``` bash
go build -o scdbclient cmd/scdbclient/main.go
# try scdbclient
./scdbclient --help
```

## Commands

The following commands are available in the SCDBClient:

* help: Provides help about any command.
* prompt: Enables interactive query mode, allowing the user to issue queries to the SCDB server.
* source: Executes SCQL statements in a script file.

## Flags

The following flags are available in the SCDBClient for customizing its behavior

```
--host: SCDB server host (default "http://localhost:8080").
--userName: User name for SCDB user.
--passwd: User password for SCDB user.
--usersConfFileName: User conf file name (default "cmd/scdbclient/users.json")
--sourceFile: scql statement script file
--sync: Determines whether queries are executed in synchronous or asynchronous mode (default asynchronous).
--pollingTimes: Polling times. If pollingTimes <=0, polling until the result is obtained.
--pollingTimeIntervalSeconds: Polling times interval in seconds (default 1s).
```

## Usage

To use the SCDBClient, simply run the client with a command and some relevant flags.

### Configure user from config file

To submit a query to SCDB, the client needs to provide necessary [user information](../../api/scdb_api.proto), we can use usersConfFileName parameter to refer to a file, such as [user.json](users.json), which contains pre-filled user information that correspond to the user information stored in the SCQL system.

``` bash
$ ./scdbclient prompt --host=http://example.com --usersConfFileName=/path/to/user.json

> switch alice
alice> select ...
```

### Configure user from cmd line

In addition to read user information from json file, The client also supports filling in user information via the command line.

``` bash
# please fill your user name, password in this command
$ ./scdbclient prompt --host=http://example.com --userName= --passwd= 

alice> select ...
```

### Execute queries from a sql file

Except work in interactive mode, it is also possible for client to read quires from a sql file that contains the statements you wish to execute. To do this you need to use `source` and `sourceFile` as follows:

``` bash
./scdbclient source --host=http://example.com --usersConfFileName=users.json --sourceFile=/path/to/script.sql
```

### Execute queries in sync/async mode

The SCDB server provides two APIs, namely [Submit](../../docs/development/scql_api.md#submit) and [SubmitAndGet](../../docs/development/scql_api.md#submitandget), to allow clients to submit queries. Clients can specify their preferred option by using the `sync` flag

``` bash
# Submit (default)
./scdbclient prompt --host=http://example.com --usersConfFileName=users.json
# SubmitAndGet
./scdbclient prompt --host=http://example.com --usersConfFileName=users.json --sync
```

### Exit Client

Type `exit` or `quit` to exit
