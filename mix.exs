defmodule Stargate.MixProject do
  use Mix.Project

  def project() do
    [
      app: :stargate,
      version: "0.1.0",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      package: package(),
      description: description(),
      source_url: "https://github.com/jeffgrunewald/stargate",
      elixirc_paths: elixirc_paths(Mix.env()),
      test_paths: test_paths(Mix.env()),
      dialyzer: [plt_file: {:no_warn, ".dialyzer/dialyzer-#{System.version()}.plt"}]
    ]
  end

  def application() do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps() do
    [
      {:dialyxir, "~> 1.0.0-rc.7", only: :dev, runtime: false},
      {:divo, "~> 1.1", only: [:dev, :integration]},
      {:divo_pulsar, "~> 0.1.1", only: [:dev, :integration]},
      {:ex_doc, "~> 0.21.0", only: :dev},
      {:gen_stage, "~> 0.14.0"},
      {:jason, "~> 1.1"},
      {:plug_cowboy, "~> 2.1.0", only: [:test, :integration]},
      {:puid, "~> 1.0"},
      {:websockex, "~> 0.4.0"}
    ]
  end

  defp elixirc_paths(env) when env in [:test, :integration], do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp test_paths(:integration), do: ["test/integration"]
  defp test_paths(_), do: ["test/unit"]

  defp description(),
    do: "An Apache Pulsar client written in Elixir using the Pulsar websocket API."

  defp package() do
    [
      maintainers: ["jeffgrunewald"],
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => "https://github.com/jeffgrunewald/stargate"}
    ]
  end

  defp docs() do
    [
      main: "readme",
      source_url: "https://github.com/jeffgrunewald/stargate",
      extras: [
        "README.md"
      ]
    ]
  end
end
