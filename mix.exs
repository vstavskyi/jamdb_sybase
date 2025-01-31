defmodule Jamdb.Sybase.Mixfile do
  use Mix.Project

  def project do
    [app: :jamdb_sybase,
     version: "0.7.15",
     elixir: "~> 1.11",
     description: description(),
     package: package(),
     deps: deps()]
  end

  defp deps do
    [
      {:ecto_sql, "~> 3.12"}
    ]
  end

  defp description do
    "Erlang driver and Ecto adapter for Sybase"
  end

  defp package do
    [files: ["src","include","lib","mix.exs"],
    maintainers: ["Sergiy Kostyushkin","Mykhailo Vstavskyi"],
    licenses: ["MIT"],
    links: %{"Github" => "https://github.com/erlangbureau/jamdb_sybase"}]
  end
end
