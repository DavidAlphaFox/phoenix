defmodule Phoenix.Router.Helpers do
  # Module that generates the routing helpers.
  @moduledoc false

  alias Phoenix.Router.Route
  alias Phoenix.Socket
  alias Plug.Conn

  @doc """
  Callback invoked by url generated in each helper module.
  """
  def url(_router, %Conn{private: private}) do
    private.phoenix_endpoint.url
  end

  def url(_router, %Socket{endpoint: endpoint}) do
    endpoint.url
  end

  def url(_router, %URI{} = uri) do
    uri_to_string(uri)
  end

  def url(_router, endpoint) when is_atom(endpoint) do
    endpoint.url
  end

  @doc """
  Callback invoked by path generated in each helper module.
  """
  def path(router, %Conn{} = conn, path) do
    conn
    |> build_own_forward_path(router, path)
    |> Kernel.||(build_conn_forward_path(conn, router, path))
    |> Kernel.||(path_with_script(path, conn.script_name))
  end

  def path(_router, %URI{} = uri, path) do
    (uri.path || "") <> path
  end

  def path(_router, %Socket{endpoint: endpoint}, path) do
    endpoint.path(path)
  end

  def path(_router, endpoint, path) when is_atom(endpoint) do
    endpoint.path(path)
  end

  ## Helpers

  defp uri_to_string(uri) do
    scheme = uri.scheme

    if scheme && (port = URI.default_port(scheme)) do
      if uri.port == port, do: uri = %{uri | port: nil}
    end

    authority = extract_authority(uri)

    result = ""
    if uri.scheme, do: result = result <> uri.scheme <> ":"
    if authority,  do: result = result <> "//" <> authority
    result
  end

  defp extract_authority(%{host: nil, authority: authority}) do
    authority
  end

  defp extract_authority(%{host: host, userinfo: userinfo, port: port}) do
    authority = host
    if userinfo, do: authority = userinfo <> "@" <> authority
    if port, do: authority = authority <> ":" <> Integer.to_string(port)
    authority
  end

  defp build_own_forward_path(conn, router, path) do
    case Map.fetch(conn.private, router) do
      {:ok, {local_script, _}} ->
        path_with_script(path, local_script)
      :error -> nil
    end
  end

  defp build_conn_forward_path(%Conn{private: %{phoenix_router: phx_router}} = conn, router, path) do
    case Map.fetch(conn.private, phx_router) do
      {:ok, {script_name, forwards}} ->
        case Map.fetch(forwards, router) do
          {:ok, local_script} ->
            path_with_script(path, script_name ++ local_script)
          :error -> nil
        end
      :error -> nil
    end
  end
  defp build_conn_forward_path(_conn, _router, _path), do: nil

  defp path_with_script(path, []) do
    path
  end
  defp path_with_script(path, script) do
    "/" <> Enum.join(script, "/") <> path
  end

  # 让helpers模块生成相应的函数
  @doc """
  Generates the helper module for the given environment and routes.
  """
  def define(env, routes) do
    ast = for {route, exprs} <- routes, do: defhelper(route, exprs)

    # It is in general bad practice to generate large chunks of code
    # inside quoted expressions. However, we can get away with this
    # here for two reasons:
    #
    # * Helper modules are quite uncommon, typically one per project.
    #
    # * We inline most of the code for performance, so it is specific
    #   per helper module anyway.
    #
    code = quote do
      @moduledoc """
      Module with named helpers generated from #{inspect unquote(env.module)}.
      """
      unquote(ast)

      @doc """
      Generates the connection/endpoint base URL without any path information.
      """
      def url(data) do
        Phoenix.Router.Helpers.url(unquote(env.module), data)
      end

      @doc """
      Generates the path information including any necessary prefix.
      """
      def path(data, path) do
        Phoenix.Router.Helpers.path(unquote(env.module), data, path)
      end

      @doc """
      Generates path to a static asset given its file path.
      """
      def static_path(%Conn{private: private} = conn, path) do
        private.phoenix_endpoint.static_path(path)
      end

      def static_path(%Socket{endpoint: endpoint} = conn, path) do
        endpoint.static_path(path)
      end

      def static_path(endpoint, path) when is_atom(endpoint) do
        endpoint.static_path(path)
      end

      @doc """
      Generates url to a static asset given its file path.
      """
      def static_url(%Conn{private: private} = conn, path) do
        static_url(private.phoenix_endpoint, path)
      end

      def static_url(%Socket{endpoint: endpoint} = conn, path) do
        static_url(endpoint, path)
      end

      def static_url(endpoint, path) when is_atom(endpoint) do
        endpoint.static_url <> endpoint.static_path(path)
      end

      # Functions used by generated helpers
      # Those are inlined here for performance

      defp to_param(int) when is_integer(int), do: Integer.to_string(int)
      defp to_param(bin) when is_binary(bin), do: bin
      defp to_param(false), do: "false"
      defp to_param(true), do: "true"
      defp to_param(data), do: Phoenix.Param.to_param(data)

      defp segments(segments, [], _reserved) do
        segments
      end

      defp segments(segments, query, reserved) do
        dict = for {k, v} <- query,
               not (k = to_string(k)) in reserved,
               do: {k, v}

        case Conn.Query.encode dict, &to_param/1 do
          "" -> segments
          o  -> segments <> "?" <> o
        end
      end
    end

    Module.create(Module.concat(env.module, Helpers), code,
                  line: env.line, file: env.file)
  end

  @doc """
  Receives a route and returns the quoted definition for its helper function.

  In case a helper name was not given, or route is forwarded, returns nil.
  """
  def defhelper(%Route{helper: nil}, _exprs), do: nil
  def defhelper(%Route{kind: :forward}, _exprs), do: nil
  def defhelper(%Route{} = route, exprs) do
    helper = route.helper
    opts = route.opts

    {bins, vars} = :lists.unzip(exprs.binding)
    segs = expand_segments(exprs.path)

    # We are using -1 to avoid warnings in case a path has already been defined.
    quote line: -1 do
      def unquote(:"#{helper}_path")(conn_or_endpoint, unquote(opts), unquote_splicing(vars)) do
        unquote(:"#{helper}_path")(conn_or_endpoint, unquote(opts), unquote_splicing(vars), [])
      end

      def unquote(:"#{helper}_path")(conn_or_endpoint, unquote(opts), unquote_splicing(vars), params) do
        path(conn_or_endpoint, segments(unquote(segs), params, unquote(bins)))
      end

      def unquote(:"#{helper}_url")(conn_or_endpoint, unquote(opts), unquote_splicing(vars)) do
        unquote(:"#{helper}_url")(conn_or_endpoint, unquote(opts), unquote_splicing(vars), [])
      end

      def unquote(:"#{helper}_url")(conn_or_endpoint, unquote(opts), unquote_splicing(vars), params) do
        url(conn_or_endpoint) <> unquote(:"#{helper}_path")(conn_or_endpoint, unquote(opts), unquote_splicing(vars), params)
      end
    end
  end

  defp expand_segments([]), do: "/"
  defp expand_segments(segments) when is_list(segments),
    do: expand_segments(segments, "")
  defp expand_segments(segments) do
    quote(do: "/" <> Enum.map_join(unquote(segments), "/", &URI.encode_www_form/1))
  end

  defp expand_segments([{:|, _, [h, t]}], acc),
    do: quote(do: unquote(expand_segments([h], acc)) <> "/" <> Enum.map_join(unquote(t), "/", &URI.encode_www_form/1))

  defp expand_segments([h|t], acc) when is_binary(h),
    do: expand_segments(t, quote(do: unquote(acc) <> unquote("/" <> h)))
  defp expand_segments([h|t], acc),
    do: expand_segments(t, quote(do: unquote(acc) <> "/" <> URI.encode_www_form(to_param(unquote(h)))))
  defp expand_segments([], acc),
    do: acc
end
