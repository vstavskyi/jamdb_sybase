defmodule Jamdb.SybaseTest do
  use ExUnit.Case, async: true

  import Ecto.Query

  alias Ecto.Queryable
  alias Ecto.Adapters.Jamdb.Sybase.Connection, as: SQL
  alias Ecto.Migration.Reference

  defmodule Schema do
    use Ecto.Schema

    schema "schema" do
      field :x, :integer
      field :y, :integer
      field :z, :integer
      field :w, :decimal

      has_many :comments, Jamdb.SybaseTest.Schema2,
        references: :x,
        foreign_key: :z

      has_one :permalink, Jamdb.SybaseTest.Schema3,
        references: :y,
        foreign_key: :id
    end
  end

  defmodule Schema2 do
    use Ecto.Schema

    import Ecto.Query

    schema "schema2" do
      belongs_to :post, Jamdb.SybaseTest.Schema,
        references: :x,
        foreign_key: :z
    end
  end

  defmodule Schema3 do
    use Ecto.Schema

    import Ecto.Query

    @schema_prefix "foo"
    schema "schema3" do
      field :binary, :binary
    end
  end

  defp plan(query, operation \\ :all) do
    Ecto.Adapter.Queryable.plan_query(operation, Jamdb.Sybase, query) |> elem(0)
  end

  defp all(query), do: query |> SQL.all() |> IO.iodata_to_binary()
  defp update_all(query), do: query |> SQL.update_all() |> IO.iodata_to_binary()
  defp delete_all(query), do: query |> SQL.delete_all() |> IO.iodata_to_binary()
  defp execute_ddl(query), do: query |> SQL.execute_ddl() |> Enum.map(&IO.iodata_to_binary/1)

  defp insert(prefx, table, header, rows, on_conflict, returning, placeholders \\ []) do
    IO.iodata_to_binary(
      SQL.insert(prefx, table, header, rows, on_conflict, returning, placeholders)
    )
  end

  defp update(prefx, table, fields, filter, returning) do
    IO.iodata_to_binary(SQL.update(prefx, table, fields, filter, returning))
  end

  defp delete(prefx, table, filter, returning) do
    IO.iodata_to_binary(SQL.delete(prefx, table, filter, returning))
  end

  test "from" do
    query = Schema |> select([r], r.x) |> plan()
    assert all(query) == ~s{SELECT s0.x FROM schema s0}
  end

  defp remove_newlines(string) when is_binary(string) do
    string |> String.trim() |> String.replace("\n", " ")
  end
end
