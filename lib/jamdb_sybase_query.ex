defmodule Jamdb.Sybase.Query do
  @moduledoc """
  Adapter module for Sybase. `DBConnection.Query` protocol implementation.

  See `DBConnection.prepare_execute/4`.

  """

  defstruct [:statement, :name]  

  @parent_as __MODULE__
  alias Ecto.Query.{BooleanExpr, ByExpr, JoinExpr, QueryExpr}

  @doc false
  def all(query, as_prefix \\ []) do
    sources = create_names(query, as_prefix)

    from = from(query, sources)
    select = select(query, sources)
    join = join(query, sources)
    where = where(query, sources)
    group_by = group_by(query, sources)
    having = having(query, sources)
    combinations = combinations(query)
    order_by = order_by(query, sources)
    lock = lock(query.lock)

    [select, from, join, where, group_by, having, combinations, order_by | lock]
  end

  @doc false
  def update_all(%{from: %{source: source}} = query, prefix \\ nil) do
    sources = create_names(query, [])
    {from, name} = get_source(query, sources, 0, source)

    prefix = prefix || ["UPDATE ", from | " SET "]
    fields = update_fields(query, sources)
    where = where(%{query | wheres: query.wheres}, sources)

    [prefix, fields, " FROM ", from, ?\s, name, where]
  end

  @doc false
  def delete_all(%{from: from} = query) do
    sources = create_names(query, [])
    {from, name} = get_source(query, sources, 0, from)

    where = where(%{query | wheres: query.wheres}, sources)

    ["DELETE ", from, " FROM ", from, ?\s, name, where]
  end

  @doc false
  def insert(prefix, table, header, rows, _on_conflict, _returning, placeholders \\ []) do
    counter_offset = length(placeholders) + 1
    values =
      if header == [] do
        [?\s | insert_all(rows, counter_offset)]
      else
        [?\s, ?(, intersperse_map(header, ?,, &quote_name/1), ?),
         ?\s | insert_all(rows, counter_offset)]
      end

    ["INSERT INTO ", quote_table(prefix, table), values]
  end

  defp insert_all(query = %Ecto.Query{}, _counter) do
    [?(, all(query), ?)]
  end

  defp insert_all(rows, counter) do
    ["VALUES ", intersperse_reduce(rows, ?,, counter, fn row, counter ->
      {row, counter} = insert_each(row, counter)
      {[?(, row, ?)], counter}
    end)
    |> elem(0)]
  end

  defp insert_each(values, counter) do
    intersperse_reduce(values, ?,, counter, fn
      nil, counter ->
        {"DEFAULT", counter}

      {%Ecto.Query{} = query, params_counter}, counter ->
        {[?(, all(query), ?)], counter + params_counter}

      {:placeholder, _}, counter ->
        {'?', counter}

      _, counter ->
        {'?', counter + 1}
    end)
  end  

  @doc false
  def update(prefix, table, fields, filters, _returning) do
    {fields, count} = intersperse_reduce(fields, ", ", 1, fn field, acc ->
      {[quote_name(field), " = ?"], acc + 1}
    end)

    {filters, _count} = intersperse_reduce(filters, " AND ", count, fn
      {field, nil}, acc ->
        {[quote_name(field), " IS NULL"], acc}

      {field, _value}, acc ->
        {[quote_name(field), " = ?"], acc + 1}
    end)

    ["UPDATE ", quote_table(prefix, table), " SET ",
     fields, " WHERE ", filters]
  end

  @doc false
  def delete(prefix, table, filters, _returning) do
    {filters, _} = intersperse_reduce(filters, " AND ", 1, fn
      {field, nil}, acc ->
        {[quote_name(field), " IS NULL"], acc}

      {field, _value}, acc ->
        {[quote_name(field), " = ?"], acc + 1}
    end)

    ["DELETE FROM ", quote_table(prefix, table), " WHERE ", filters]
  end

  @doc false
  def table_exists_query(table) do
    {"SELECT count(*) FROM sysobjects WHERE name = ? AND type='U'", [table]}
  end

  @doc false
  def execute_ddl(_command),
    do: error!(nil, "execute ddl is not supported")

  @doc false
  def ddl_logs(_result), do: []

  @doc false
  def to_constraints(_err, _opts \\ []), do: []

  ## Query generation

  binary_ops =
    [==: " = ", !=: " != ", <=: " <= ", >=: " >= ", <: " < ", >: " > ",
     +: " + ", -: " - ", *: " * ", /: " / ",
     and: " AND ", or: " OR ", like: " LIKE "]

  @binary_ops Keyword.keys(binary_ops)

  Enum.map(binary_ops, fn {op, str} ->
    defp handle_call(unquote(op), 2), do: {:binary_op, unquote(str)}
  end)

  defp handle_call(fun, _arity), do: {:fun, Atom.to_string(fun)}

  defp select(%{select: %{fields: fields}, distinct: distinct} = query, sources) do
    ["SELECT ", limit(query, sources), distinct(distinct, sources, query) |
                select_fields(fields, sources, query)]
  end

  defp select_fields([], _sources, _query),
    do: "1"
  defp select_fields(fields, sources, query) do
    intersperse_map(fields, ", ", fn
      {:&, _, [idx]} ->
        case elem(sources, idx) do
          {_, source, nil} -> [source, ?. | "*"]
          {_, source, _} -> source
        end
      {key, value} ->
        [expr(value, sources, query), ?\s | quote_name(key)]
      value ->
        expr(value, sources, query)
    end)
  end

  defp distinct(nil, _, _), do: []
  defp distinct(%ByExpr{expr: []}, _, _), do: {[], []}
  defp distinct(%ByExpr{expr: true}, _, _), do: "DISTINCT "
  defp distinct(%ByExpr{expr: false}, _, _), do: []
  defp distinct(%ByExpr{expr: exprs}, _, _) when is_list(exprs), do: "DISTINCT "

  defp from(%{from: %{hints: [_ | _]}} = query, _sources) do
    error!(query, "table hints are not supported")
  end

  defp from(%{from: %{source: source}} = query, sources) do
    {from, name} = get_source(query, sources, 0, source)
    [" FROM ", from, ?\s | name]
  end

  defp update_fields(%{updates: updates} = query, sources) do
    for(%{expr: expr} <- updates,
        {op, kw} <- expr,
        {key, value} <- kw,
        do: update_op(op, key, value, sources, query)) |> Enum.intersperse(", ")
  end

  defp update_op(:set, key, value, sources, query) do
    [quote_name(key), " = " | expr(value, sources, query)]
  end

  defp update_op(:inc, key, value, sources, query) do
    [quote_name(key), " = ", quote_qualified_name(key, sources, 0), " + " | 
	 expr(value, sources, query)]
  end

  defp update_op(command, _quoted_key, _value, _sources, query) do
    error!(query, "Unknown update operation #{inspect command}")
  end

  defp join(%{joins: []}, _sources), do: []
  defp join(%{joins: joins} = query, sources) do
    [?\s | intersperse_map(joins, ?\s, fn
      %JoinExpr{on: %QueryExpr{expr: expr}, qual: qual, ix: ix, source: source, hints: hints} ->
        if hints != [] do
          error!(query, "table hints are not supported")
        end

        {join, name} = get_source(query, sources, ix, source)
        [join_qual(qual), join, ?\s, name | join_on(qual, expr, sources, query)]
    end)]
  end

  defp join_on(:cross, true, _sources, _query), do: []
  defp join_on(_qual, true, _sources, _query), do: [" ON 1 = 1"]
  defp join_on(_qual, expr, sources, query), do: [" ON " | expr(expr, sources, query)]

  defp join_qual(:inner), do: "INNER JOIN "
  defp join_qual(:left),  do: "LEFT OUTER JOIN "
  defp join_qual(:right), do: "RIGHT OUTER JOIN "

  defp where(%{wheres: wheres} = query, sources) do
    boolean(" WHERE ", wheres, sources, query)
  end

  defp having(%{havings: havings} = query, sources) do
    boolean(" HAVING ", havings, sources, query)
  end

  defp group_by(%{group_bys: []}, _sources), do: []
  defp group_by(%{group_bys: group_bys} = query, sources) do
    [" GROUP BY " |
     intersperse_map(group_bys, ", ", fn %ByExpr{expr: expr} ->
         intersperse_map(expr, ", ", &expr(&1, sources, query))
     end)]
  end

  defp order_by(%{order_bys: []}, _sources), do: []
  defp order_by(%{order_bys: order_bys} = query, sources) do
    [" ORDER BY " |
     intersperse_map(order_bys, ", ", fn %ByExpr{expr: expr} ->
         intersperse_map(expr, ", ", &order_by_expr(&1, sources, query))
     end)]
  end

  defp order_by_expr({dir, expr}, sources, query) do
    str = expr(expr, sources, query)
    case dir do
      :asc  -> str
      :desc -> [str | " DESC"]
    end
  end

  defp limit(%{limit: nil}, _sources), do: []
  defp limit(%{limit: %{expr: expr}} = query, sources) do
    [" TOP ", expr(expr, sources, query), ?\s]
  end

  defp combinations(%{combinations: combinations}) do
    Enum.map(combinations, fn
      {:union, query} -> [" UNION (", all(query), ")"]
      {:union_all, query} -> [" UNION ALL (", all(query), ")"]
    end)
  end

  defp lock(nil), do: []
  defp lock(lock_clause), do: [?\s | lock_clause]

  defp boolean(_name, [], _sources, _query), do: []
  defp boolean(name, [%{expr: expr, op: op} | query_exprs], sources, query) do
    [name |
     Enum.reduce(query_exprs, {op, paren_expr(expr, sources, query)}, fn
       %BooleanExpr{expr: expr, op: op}, {op, acc} ->
         {op, [acc, operator_to_boolean(op), paren_expr(expr, sources, query)]}
       %BooleanExpr{expr: expr, op: op}, {_, acc} ->
         {op, [?(, acc, ?), operator_to_boolean(op), paren_expr(expr, sources, query)]}
     end) |> elem(1)]
  end

  defp operator_to_boolean(:and), do: " AND "
  defp operator_to_boolean(:or), do: " OR "

  defp parens_for_select([first_expr | _] = expr) do
    if is_binary(first_expr) and String.starts_with?(first_expr, ["SELECT", "select"]) do
      [?(, expr, ?)]
    else
      expr
    end
  end

  defp paren_expr(expr, sources, query) do
    [?(, expr(expr, sources, query), ?)]
  end

  defp expr({:^, [], [_ix]}, _sources, _query) do
    '?'
  end

  defp expr({{:., _, [{:parent_as, _, [as]}, field]}, _, []}, _sources, query)
       when is_atom(field) do
    {ix, sources} = get_parent_sources_ix(query, as)
    quote_qualified_name(field, sources, ix)
  end

  defp expr({{:., _, [{:&, _, [idx]}, field]}, _, []}, sources, _query) when is_atom(field) do
    quote_qualified_name(field, sources, idx)
  end

  defp expr({:&, _, [idx]}, sources, _query) do
    {_, source, _} = elem(sources, idx)
    source
  end

  defp expr({:in, _, [_left, []]}, _sources, _query) do
    "0"
  end

  defp expr({:in, _, [left, right]}, sources, query) when is_list(right) do
    args = intersperse_map(right, ?,, &expr(&1, sources, query))
    [expr(left, sources, query), " IN (", args, ?)]
  end

  defp expr({:in, _, [_, {:^, _, [_, 0]}]}, _sources, _query) do
    "0"
  end

  defp expr({:in, _, [left, {:^, _, [_, length]}]}, sources, query) do
    args = Enum.intersperse(List.duplicate(??, length), ?,)
    [expr(left, sources, query), " IN (", args, ?)]
  end

  defp expr({:in, _, [left, %Ecto.SubQuery{} = subquery]}, sources, query) do
    [expr(left, sources, query), " IN ", expr(subquery, sources, query)]
  end

  defp expr({:in, _, [left, right]}, sources, query) do
    [expr(left, sources, query), " = ANY(", expr(right, sources, query), ?)]
  end

  defp expr({:is_nil, _, [arg]}, sources, query) do
    [expr(arg, sources, query) | " IS NULL"]
  end

  defp expr({:not, _, [expr]}, sources, query) do
    ["NOT (", expr(expr, sources, query), ?)]
  end

  defp expr(%Ecto.SubQuery{query: query}, sources, parent_query) do
    query = put_in(query.aliases[@parent_as], {parent_query, sources})
    [?(, all(query, subquery_as_prefix(sources)), ?)]
  end

  defp expr({:fragment, _, [kw]}, _sources, query) when is_list(kw) or tuple_size(kw) == 3 do
    error!(query, "keyword or interpolated fragments are not supported")
  end

  defp expr({:fragment, _, parts}, sources, query) do
    Enum.map(parts, fn
      {:raw, part}  -> part
      {:expr, expr} -> expr(expr, sources, query)
    end)
    |> parens_for_select
  end

  defp expr({:date_add, _, [date, count, interval]}, sources, query) do
    interval(date, count, interval, sources, query)
  end

  defp expr({:datetime_add, _, [datetime, count, interval]}, sources, query) do
    interval(datetime, count, interval, sources, query)
  end

  defp expr({:from_now, _, [count, interval]}, sources, query) do
    interval(DateTime.utc_now, count, interval, sources, query)
  end

  defp expr({:ago, _, [count, interval]}, sources, query) do
    interval(DateTime.utc_now, count, interval, sources, query)
  end

  defp expr({:filter, _, _}, _sources, query) do
    error!(query, "aggregate filters are not supported")
  end

  defp expr({:{}, _, elems}, sources, query) do
    [?(, intersperse_map(elems, ?,, &expr(&1, sources, query)), ?)]
  end

  defp expr({:count, _, []}, _sources, _query), do: "count(*)"
  defp expr({:count, _, [literal, :distinct]}, sources, query) do
    exprs = expr(literal, sources, query)
    ["count (", distinct(%QueryExpr{expr: exprs}, sources, query), exprs, ?)]
  end

  defp expr({fun, _, args}, sources, query) when is_atom(fun) and is_list(args) do
    {modifier, args} =
      case args do
        [rest, :distinct] -> {"DISTINCT ", [rest]}
        _ -> {[], args}
      end

    case handle_call(fun, length(args)) do
      {:binary_op, op} ->
        [left, right] = args
        [maybe_paren(left, sources, query), op | maybe_paren(right, sources, query)]
      {:fun, fun} ->
        [fun, ?(, modifier, intersperse_map(args, ", ", &expr(&1, sources, query)), ?)]
    end
  end

  defp expr(%Ecto.Query.Tagged{value: literal}, sources, query) do
     ["CAST(", expr(literal, sources, query), " AS varchar)"]
  end

  defp expr(nil, _sources, _query),   do: "NULL"
  defp expr(true, _sources, _query),  do: "1"
  defp expr(false, _sources, _query), do: "0"

  defp expr(literal, _sources, _query) when is_binary(literal) do
    ["'", escape_string(literal), "'"]
  end

  defp expr(literal, _sources, _query) when is_integer(literal) do
    Integer.to_string(literal)
  end

  defp expr(literal, _sources, _query) when is_float(literal) do
    Float.to_string(literal)
  end

  defp interval(datetime, count, interval, sources, query) do
    ["dateadd (", interval, ?,, expr(count, sources, query), ?,,
      expr(datetime, sources, query), ?)]
  end

  defp maybe_paren({op, _, [_, _]} = expr, sources, query) when op in @binary_ops,
    do: paren_expr(expr, sources, query)

  defp maybe_paren({:is_nil, _, [_]} = expr, sources, query),
    do: paren_expr(expr, sources, query)

  defp maybe_paren(expr, sources, query),
    do: expr(expr, sources, query)

  defp create_names(%{sources: sources}, as_prefix) do
    create_names(sources, 0, tuple_size(sources), as_prefix) |> List.to_tuple()
  end

  defp create_names(sources, pos, limit, as_prefix) when pos < limit do
    [create_name(sources, pos, as_prefix) | create_names(sources, pos + 1, limit, as_prefix)]
  end

  defp create_names(_sources, pos, pos, as_prefix) do
    [as_prefix]
  end

  defp subquery_as_prefix(sources) do
    [?s | :erlang.element(tuple_size(sources), sources)]
  end

  defp create_name(sources, pos, as_prefix) do
    case elem(sources, pos) do
      {:fragment, _, _} ->
        {nil, as_prefix ++ [?f | Integer.to_string(pos)], nil}

      {table, schema, prefix} ->
        name = as_prefix ++ [create_alias(table) | Integer.to_string(pos)]
        {quote_table(prefix, table), name, schema}

      %Ecto.SubQuery{} ->
        {nil, as_prefix ++ [?s | Integer.to_string(pos)], nil}
    end
  end

  defp create_alias(<<first, _rest::binary>>) when first in ?a..?z when first in ?A..?Z do
    <<first>>
  end
  defp create_alias(_) do
    "t"
  end

  ## Helpers

  defp get_source(query, sources, ix, source) do
    {expr, name, _schema} = elem(sources, ix)
    {expr || paren_expr(source, sources, query), name}
  end

  defp get_parent_sources_ix(query, as) do
    case query.aliases[@parent_as] do
      {%{aliases: %{^as => ix}}, sources} -> {ix, sources}
      {%{} = parent, _sources} -> get_parent_sources_ix(parent, as)
    end
  end

  defp quote_qualified_name(name, sources, ix) do
    {_, source, _} = elem(sources, ix)
    [source, ?. | quote_name(name)]
  end

  defp quote_name(name) when is_atom(name) do
    quote_name(Atom.to_string(name))
  end
  defp quote_name(name) do
     [name]
  end

  defp quote_table(nil, name),    do: quote_table(name)
  defp quote_table(prefix, name), do: [quote_table(prefix), ?., quote_table(name)]

  defp quote_table(name) when is_atom(name),
    do: quote_table(Atom.to_string(name))
  defp quote_table(name) do
     [name]
  end

  defp intersperse_map(list, separator, mapper, acc \\ [])
  defp intersperse_map([], _separator, _mapper, acc),
    do: acc
  defp intersperse_map([elem], _separator, mapper, acc),
    do: [acc | mapper.(elem)]
  defp intersperse_map([elem | rest], separator, mapper, acc),
    do: intersperse_map(rest, separator, mapper, [acc, mapper.(elem), separator])

  defp intersperse_reduce(list, separator, user_acc, reducer, acc \\ [])
  defp intersperse_reduce([], _separator, user_acc, _reducer, acc),
    do: {acc, user_acc}
  defp intersperse_reduce([elem], _separator, user_acc, reducer, acc) do
    {elem, user_acc} = reducer.(elem, user_acc)
    {[acc | elem], user_acc}
  end
  defp intersperse_reduce([elem | rest], separator, user_acc, reducer, acc) do
    {elem, user_acc} = reducer.(elem, user_acc)
    intersperse_reduce(rest, separator, user_acc, reducer, [acc, elem, separator])
  end

  defp escape_string(value) when is_list(value) do
    escape_string(:binary.list_to_bin(value))
  end
  defp escape_string(value) when is_binary(value) do
    :binary.replace(value, "'", "''", [:global])
  end

  defp error!(nil, msg) do
    raise ArgumentError, msg
  end
  defp error!(query, msg) do
    raise Ecto.QueryError, query: query, message: msg
  end
  
end

defimpl String.Chars, for: Jamdb.Sybase.Query do
  def to_string(%Jamdb.Sybase.Query{statement: statement}) do
    IO.iodata_to_binary(statement)
  end
end
