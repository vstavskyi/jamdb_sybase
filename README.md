[![Build Status](https://travis-ci.org/vstavskyi/jamdb_sybase-1.svg?branch=ecto-adapter)](https://travis-ci.org/vstavskyi/jamdb_sybase-1)

# Jamdb.Sybase

Erlang driver and Ecto adapter for Sybase ASE Database

## Options

Adapter options split in different categories described
below. All options can be given via the repository
configuration:

    config :your_app, YourApp.Repo,
      ...

### Connection options

  * `:hostname` - Server hostname (Name or IP address of the database server)
  * `:port` - Server port (Number of the port where the server listens for requests)
  * `:database` - Database (Database name)
  * `:username` - Username (Name for the connecting user)
  * `:password` - User password (Password for the connecting user)
  * `:parameters` - Keyword list of connection parameters
  * `:socket_options` - Options to be given to the underlying socket
  * `:timeout` - The default timeout to use on queries, defaults to `15000`

### Pool options

  * `:pool` - The connection pool module, defaults to `DBConnection.ConnectionPool`
  * `:pool_size` - The size of the pool, defaults to `1`
  * `:idle_interval` - The ping interval to validate an idle connection, defaults to `1000`	

### Input parameters

Using query options: `[in: [:numeric, :binary]]`

Sybase types                        | Literal syntax in params
:---------------------------------- | :-----------------------
`bigint`,`int`,`smallint`,`tinyint` | `:int`, `:integer`
`numeric`, `decimal`                | `:numeric`, `:decimal`
`float`, `double`                   | `:float`, `:double`
`char`, `varchar`, `univarchar`     | `:varchar`, `:char`, `:string`
`nchar`, `nvarchar`                 | `:nvarchar`, `:nchar`
`text`, `unitext`, `image`          | `:text`, `:clob`
`binary`, `varbinary`               | `:binary`
`long binary`, ` long varchar`      | `:long`
`money`, `smallmoney`               | `:money`
`datetime`, `smalldatetime`         | `:datetime`
`date`                              | `:date`
`time`                              | `:time`
`bit`                               | `:bit`

#### Examples

    iex> Ecto.Adapters.SQL.query(YourApp.Repo, "select 1/10, getdate(), newid()", [])
    {:ok, %{num_rows: 1, rows: [[0, ~N[2016-08-01 13:14:15], "84be3f476ce14052aed69dbfa57cdd43"]]}}

    iex> bin = %Ecto.Query.Tagged{value: <<0xE7,0x99,0xBE>>, type: :binary}
    iex> Ecto.Adapters.SQL.query(YourApp.Repo, "insert into tabl values (:1)", [bin])

    iex> bin = <<0xE7,0x99,0xBE>>
    iex> Ecto.Adapters.SQL.query(YourApp.Repo, "insert into tabl values (:1)", [bin]], [in: [:binary]])
