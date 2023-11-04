defmodule Jamdb.Sybase do
  @vsn "0.7.13"
  @moduledoc """
  Adapter module for Sybase. `DBConnection` behaviour implementation.

  It uses `jamdb_sybase` for communicating to the database.

  """

  use DBConnection

  @timeout 15_000
  @idle_interval 5_000

  defstruct [:conn, :mode, :cursors, :timeout, :idle_interval]

  @doc """
  Starts and links to a database connection process.

  See [`Ecto.Adapters.Jamdb.Sybase`](Ecto.Adapters.Jamdb.Sybase.html#module-connection-options).

  By default the `DBConnection` starts a pool with a single connection.
  The size of the pool can be increased with `:pool_size`. The ping interval 
  to validate an idle connection can be given with the `:idle_interval` option.
  """
  @spec start_link(opts :: Keyword.t) :: 
    {:ok, any()} | {:error, any()}
  def start_link(opts) do
    DBConnection.start_link(Jamdb.Sybase, opts)
  end

  @doc """
  Runs the SQL statement.

  See `DBConnection.prepare_execute/4`.

  In case of success, it must return an `:ok` tuple containing
  a map with at least two keys:

    * `:num_rows` - the number of rows affected
    * `:rows` - the result set as a list  
  """
  @spec query(conn :: any(), sql :: any(), params :: any()) ::
    {:ok, any()} | {:error | :disconnect, any()}
  def query(conn, sql, params \\ [])
  def query(%{conn: conn, timeout: timeout} = s, sql, params) do
    case sql_query(conn, sql, params, timeout) do
      {:ok, [{:result_set, columns, _, rows}], conn} ->
        {:ok, %{num_rows: length(rows), rows: rows, columns: columns}, %{s | conn: conn}}
      {:ok, [{:proc_result, 0, rows}], conn} -> {:ok, %{num_rows: length(rows), rows: rows}, %{s | conn: conn}}
      {:ok, [{:proc_result, _, msg}], conn} -> {:error, msg, %{s | conn: conn}}
      {:ok, [{:affected_rows, num_rows}], conn} -> {:ok, %{num_rows: num_rows, rows: nil}, %{s | conn: conn}}
      {:ok, result, conn} -> {:ok, result, %{s | conn: conn}}
      {:error, err, conn} -> {:disconnect, err, conn}
    end
  end

  defp sql_query(conn, sql, [], timeout) do
    try do
      case :jamdb_sybase_conn.sql_query(conn, sql, timeout) do
        {:ok, result, conn} -> {:ok, result, conn}
        {:error, _, err, conn} -> {:error, err, conn}
      end
    catch
      _, err -> {:error, err, conn}
    end
  end
  defp sql_query(conn, sql, params, timeout) do
    try do
      name = "dyn" <> Integer.to_string(:erlang.crc32(sql))
      {:ok, conn} = :jamdb_sybase_conn.prepare(conn, name, sql)
      case :jamdb_sybase_conn.execute(conn, name, params, timeout) do
        {:ok, result, conn} -> 
          {:ok, conn} = :jamdb_sybase_conn.unprepare(conn, name)
          {:ok, result, conn}
        {:error, _, err, conn} -> {:error, err, conn}
      end
    catch
      _, err -> {:error, err, conn}
    end
  end

  @impl true
  def connect(opts) do
    host = opts[:hostname] |> Jamdb.Sybase.to_list
    port = opts[:port]
    timeout = opts[:timeout] || @timeout
    idle_interval = opts[:idle_interval] || @idle_interval
    user = opts[:username] |> Jamdb.Sybase.to_list
    password = opts[:password] |> Jamdb.Sybase.to_list
    database = opts[:database] |> Jamdb.Sybase.to_list
    env = [host: host, port: port, timeout: timeout, idle_interval: idle_interval,
           user: user, password: password, database: database]
    params = opts[:parameters] || []
    sock_opts = opts[:socket_options] || []
    case :jamdb_sybase_conn.connect(sock_opts ++ params ++ env) do
      {:ok, conn} -> {:ok, %Jamdb.Sybase{conn: conn, mode: :idle, timeout: timeout, idle_interval: idle_interval}}
      {:ok, msg, _conn} -> {:error, error!(msg)}
      {:error, _, err, _conn} -> {:error, error!(err)}
    end
  end

  @impl true
  def disconnect(_err, %{conn: conn}) do
    try do
      :jamdb_sybase_conn.disconnect(conn)
    catch
      _, _ -> :error
    end
    :ok
  end

  @impl true
  def handle_execute(query, params, _opts, s) do
    %Jamdb.Sybase.Query{statement: statement} = query
    case query(s, statement |> Jamdb.Sybase.to_list, params) do
      {:ok, result, s} -> {:ok, query, result, s}
      {:error, err, s} -> {:error, error!(err), s}
      {:disconnect, err, s} -> {:disconnect, error!(err), s}
    end
  end

  @impl true
  def handle_prepare(query, opts, s) do
    timeout = opts[:timeout] || @timeout
    {:ok, query, %{s | timeout: timeout}}
  end

  @impl true
  def handle_begin(opts, %{mode: mode} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when mode == :idle ->
        statement = "BEGIN TRANSACTION"
        handle_transaction(statement, opts, %{s | mode: :transaction})
      :savepoint when mode == :transaction ->
        statement = "SAVE TRANSACTION " <> Keyword.get(opts, :name, "svpt")
        handle_transaction(statement, opts, %{s | mode: :transaction})
      status when status in [:transaction, :savepoint] ->
        {status, s}
    end
  end

  @impl true
  def handle_commit(opts, %{mode: mode} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when mode == :transaction ->
        statement = "COMMIT TRANSACTION"
        handle_transaction(statement, opts, %{s | mode: :idle})
      :savepoint when mode == :transaction ->
        {:ok, [], %{s | mode: :transaction}}
      status when status in [:transaction, :savepoint] ->
        {status, s}
    end
  end

  @impl true
  def handle_rollback(opts, %{mode: mode} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when mode in [:transaction, :error] ->
        statement = "ROLLBACK TRANSACTION"
        handle_transaction(statement, opts, %{s | mode: :idle})
      :savepoint when mode in [:transaction, :error] ->
        statement = "ROLLBACK TRANSACTION " <> Keyword.get(opts, :name, "svpt")
        handle_transaction(statement, opts, %{s | mode: :transaction})
      status when status in [:transaction, :savepoint] ->
        {status, s}
    end
  end

  defp handle_transaction(statement, _opts, s) do
    case query(s, statement |> Jamdb.Sybase.to_list) do
      {:ok, result, s} -> {:ok, result, s}
      {:error, err, s} -> {:error, error!(err), s}
      {:disconnect, err, s} -> {:disconnect, error!(err), s}
    end
  end

  @impl true
  def handle_declare(query, params, _opts, s) do
    {:ok, query, %{params: params}, s}
  end

  @impl true
  def handle_fetch(query, %{params: params}, _opts, %{cursors: nil} = s) do
    %Jamdb.Sybase.Query{statement: statement} = query
    case query(s, statement |> Jamdb.Sybase.to_list, params) do
      {:ok, result, s} -> 
        {:halt, result, s}
      {:error, err, s} -> {:error, error!(err), s}
      {:disconnect, err, s} -> {:disconnect, error!(err), s}
    end
  end

  @impl true
  def handle_deallocate(_query, _cursor, _opts, s) do
    {:ok, nil, %{s | cursors: nil}}
  end

  @impl true
  def handle_close(_query, _opts, s) do
    {:ok, nil, s}
  end

  @impl true
  def handle_status(_opts, %{mode: mode} = s) do
    {mode, s}
  end

  @doc false
  def checkin(s) do
    {:ok, s}
  end

  @impl true
  def checkout(%{conn: conn, timeout: timeout} = s) do
    case sql_query(conn, 'SET CHAINED off', [], timeout) do
      {:ok, _, _conn} -> {:ok, s}
      {:error, err, _conn} ->  {:disconnect, error!(err), s}
    end
  end

  @impl true
  def ping(%{conn: conn, timeout: timeout, idle_interval: idle_interval} = s) do
    case sql_query(conn, 'SELECT 1', [], min(timeout, idle_interval)) do
      {:ok, _, _conn} -> {:ok, s}
      {:error, err, _conn} ->  {:disconnect, error!(err), s}
    end
  end

  defp error!(msg) do
    DBConnection.ConnectionError.exception("#{inspect msg}")
  end

  @doc """
  Returns the configured JSON library.

  To customize the JSON library, include the following in your `config/config.exs`:

      config :jamdb_sybase, :json_library, SomeJSONModule

  Defaults to [`Jason`](https://hexdocs.pm/jason)
  """
  @spec json_library() :: module()
  def json_library() do
    Application.get_env(:jamdb_sybase, :json_library, Jason)
  end

  @doc false
  def to_list(string) when is_binary(string) do
    :binary.bin_to_list(string)
  end

  @doc false
  defdelegate loaders(t, type), to: Ecto.Adapters.Jamdb.Sybase
  @doc false
  defdelegate dumpers(t, type), to: Ecto.Adapters.Jamdb.Sybase

end

defimpl DBConnection.Query, for: Jamdb.Sybase.Query do

  def parse(query, _), do: query
  def describe(query, _), do: query

  def decode(_, %{rows: []} = result, _), do: result
  def decode(_, %{rows: rows} = result, opts) when rows != nil, 
    do: %{result | rows: Enum.map(rows, fn row -> decode(row, opts[:decode_mapper]) end)}
  def decode(_, result, _), do: result

  defp decode(row, nil), do: Enum.map(row, fn elem -> decode(elem) end)
  defp decode(row, mapper), do: mapper.(decode(row, nil))

  defp decode(:null), do: nil
  defp decode(elem) when is_number(elem), do: elem
  defp decode({sign, coef, exp}) when exp < 0, do: Decimal.new(sign, coef, exp)
  defp decode({date, time}) when is_tuple(date), do: to_naive({date, time})
  defp decode(elem) when is_tuple(elem), do: to_date_time(elem)
  defp decode(elem) when is_list(elem), do: to_binary(elem)
  defp decode(elem), do: elem

  def encode(_, [], _), do: []
  def encode(_, params, opts) do
    types = Enum.map(Keyword.get(opts, :in, []), fn elem -> elem end)
    Enum.map(encode(params, types), fn elem -> encode(elem) end)
  end

  defp encode(params, []), do: params
  defp encode([%Ecto.Query.Tagged{type: :binary} = elem | next1], [_type | next2]),
    do: [ elem | encode(next1, next2)]
  defp encode([elem | next1], [type | next2]) when type in [:binary, :binary_id, Ecto.UUID],
    do: [ %Ecto.Query.Tagged{value: elem, type: :binary} | encode(next1, next2)]
  defp encode([elem | next1], [_type | next2]), do: [ elem | encode(next1, next2)]

  defp encode(nil), do: :null
  defp encode(true), do: [49]
  defp encode(false), do: [48]
  defp encode(%Decimal{} = decimal), do: Decimal.to_float(decimal)
  defp encode(%DateTime{} = datetime), do: NaiveDateTime.to_erl(DateTime.to_naive(datetime))
  defp encode(%NaiveDateTime{} = naive), do: NaiveDateTime.to_erl(naive)
  defp encode(%Date{} = date), do: Date.to_erl(date)
  defp encode(%Time{} = time), do: Time.to_erl(time)
  defp encode(%Ecto.Query.Tagged{value: elem, type: :binary}) when is_binary(elem), do: elem
  defp encode(elem) when is_binary(elem), do: Jamdb.Sybase.to_list(elem)
  defp encode(elem) when is_map(elem), 
    do: encode(Jamdb.Sybase.json_library().encode!(elem))
  defp encode(elem), do: elem

  defp expr(list) when is_list(list) do
    Enum.map(list, fn 
      :null -> nil
      elem  -> elem
    end)
  end

  defp to_binary(list) when is_list(list) do
    try do
      :binary.list_to_bin(list)
    rescue
      ArgumentError ->
        Enum.map(expr(list), fn
          elem when is_list(elem) -> expr(elem)
          other -> other
        end) |> Enum.join
    end
  end

  defp to_date_time({hour, min, sec}) when hour in 0..23 and min in 0..59 and sec in 0..60,
    do: Time.from_erl!({hour, min, sec})
  defp to_date_time(date),
    do: Date.from_erl!(date)

  defp to_naive({date, {hour, min, sec}}),
    do: NaiveDateTime.from_erl!({date, {hour, min, sec}})

end
