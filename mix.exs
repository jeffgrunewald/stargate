defmodule Stargate.MixProject do
  use Mix.Project

  @github "https://github.com/jeffgrunewald/stargate"

  def project() do
    [
      app: :stargate,
      version: "0.2.0",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      package: package(),
      description: description(),
      homepage_url: @github,
      source_url: @github,
      elixirc_paths: elixirc_paths(Mix.env()),
      test_paths: test_paths(Mix.env()),
      dialyzer: [plt_file: {:no_warn, ".dialyzer/#{System.version()}.plt"}]
    ]
  end

  def application() do
    [
      extra_applications: [:logger, :crypto]
    ]
  end

  defp deps() do
    [
      {:dialyxir, "~> 1.1", only: :dev, runtime: false},
      {:divo_pulsar, "~> 0.1.2", only: [:dev, :integration]},
      {:ex_doc, "~> 0.24", only: :dev},
      {:gen_stage, "~> 1.1"},
      {:jason, "~> 1.2"},
      {:plug_cowboy, "~> 2.5", only: [:test, :integration]},
      {:puid, "~> 1.1.1"},
      {:websockex, "~> 0.4.3"},
      {:credo, "~> 1.5", only: [:dev, :test], runtime: false}
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
      links: %{"GitHub" => @github}
    ]
  end

  defp docs() do
    [
      source_url: @github,
      extras: ["README.md"],
      source_url_pattern: "#{@github}/blob/master/%{path}#L%{line}"
    ]
  end
end
