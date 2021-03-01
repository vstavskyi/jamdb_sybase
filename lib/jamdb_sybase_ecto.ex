defmodule Ecto.Adapters.Jamdb.Sybase do
  @moduledoc """
  Adapter module for Sybase. `Ecto.Adapters.SQL` callbacks implementation.

  It uses `jamdb_sybase` for communicating to the database.

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

  """

  use Ecto.Adapters.SQL, driver: Jamdb.Sybase, migration_lock: nil

  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Structure

  @impl true
  def loaders({:array, _}, type), do: [&array_decode/1, type]
  def loaders({:embed, _}, type), do: [&json_decode/1, &Ecto.Type.embedded_load(type, &1, :json)]
  def loaders({:map, _}, type),   do: [&json_decode/1, &Ecto.Type.embedded_load(type, &1, :json)]
  def loaders(:map, type),        do: [&json_decode/1, type]
  def loaders(:float, type),      do: [&float_decode/1, type]
  def loaders(:boolean, type),    do: [&bool_decode/1, type]
  def loaders(:binary_id, type),  do: [Ecto.UUID, type]
  def loaders(_, type),           do: [type]

  defp bool_decode("0"), do: {:ok, false}
  defp bool_decode("1"), do: {:ok, true}
  defp bool_decode(0), do: {:ok, false}
  defp bool_decode(1), do: {:ok, true}
  defp bool_decode(x), do: {:ok, x}

  defp float_decode(%Decimal{} = decimal), do: {:ok, Decimal.to_float(decimal)}
  defp float_decode(x), do: {:ok, x}

  defp json_decode(x) when is_binary(x), do: {:ok, Jamdb.Sybase.json_library().decode!(x)}
  defp json_decode(x), do: {:ok, x}

  defp array_decode(x) when is_binary(x), do: {:ok, :binary.bin_to_list(x)}
  defp array_decode(x), do: {:ok, x}

  @impl true
  def storage_up(_opts), do: err()

  @impl true
  def storage_down(_opts), do: err()

  @impl true
  def storage_status(_opts), do: err()

  @impl true
  def structure_dump(_default, _config), do: err()

  @impl true
  def structure_load(_default, _config), do: err()

  @impl true
  def supports_ddl_transaction? do
    false
  end

  defp err, do: {:error, false}

end

defmodule Ecto.Adapters.Jamdb.Sybase.Connection do
  @moduledoc false

  @behaviour Ecto.Adapters.SQL.Connection

  @impl true
  def child_spec(opts) do
    DBConnection.child_spec(Jamdb.Sybase, opts)
  end

  @impl true
  def execute(conn, query, params, opts) do
    DBConnection.execute(conn, query!(query, ""), params, opts)
  end

  @impl true
  def prepare_execute(conn, name, query, params, opts) do
    DBConnection.prepare_execute(conn, query!(query, name), params, opts)
  end

  @impl true
  def stream(conn, query, params, opts) do
    DBConnection.stream(conn, query!(query, ""), params, opts)
  end

  @impl true
  def query(conn, query, params, opts) do
    case DBConnection.prepare_execute(conn, query!(query, ""), params, opts) do
      {:ok, _, result}  -> {:ok, result}
      {:error, err} -> {:error, err}
    end
  end

  @impl true
  def explain_query(_conn, _query, _params, _opts) do
    {:ok, []}
  end

  defp query!(sql, name) when is_binary(sql) or is_list(sql) do
    %Jamdb.Sybase.Query{statement: IO.iodata_to_binary(sql), name: name}
  end
  defp query!(%{} = query, _name) do
    query
  end

  defdelegate all(query), to: Jamdb.Sybase.Query
  defdelegate update_all(query), to: Jamdb.Sybase.Query
  defdelegate delete_all(query), to: Jamdb.Sybase.Query
  defdelegate insert(prefix, table, header, rows, on_conflict, returning), to: Jamdb.Sybase.Query
  defdelegate update(prefix, table, fields, filters, returning), to: Jamdb.Sybase.Query
  defdelegate delete(prefix, table, filters, returning), to: Jamdb.Sybase.Query
  defdelegate table_exists_query(table), to: Jamdb.Sybase.Query
  defdelegate execute_ddl(command), to: Jamdb.Sybase.Query
  defdelegate ddl_logs(result), to: Jamdb.Sybase.Query
  defdelegate to_constraints(err, opts), to: Jamdb.Sybase.Query

end
