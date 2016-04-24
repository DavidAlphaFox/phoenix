defmodule Phoenix.PubSub.PG2Server do
  @moduledoc false

  use GenServer
  alias Phoenix.PubSub.Local

  def start_link(name) do
    # 启动封装pg2的gen_server
    GenServer.start_link __MODULE__, name, name: name
  end

  def broadcast(name, pool_size, from_pid, topic, msg) do
    # 拿出所有进程组中的进程
    case :pg2.get_members(pg2_namespace(name)) do
      {:error, {:no_such_group, _}} ->
        {:error, :no_such_group}

      pids when is_list(pids) ->
        Enum.each(pids, fn
          pid when node(pid) == node() ->
            # 如果是本地节点，直接用Local进行广播
            Local.broadcast(name, pool_size, from_pid, topic, msg)
          pid ->
            # 如果是远程的节点的进程，发送forward消息
            # 让对方来处理
            send(pid, {:forward_to_local, from_pid, pool_size, topic, msg})
        end)
        :ok
    end
  end

  def init(name) do
    # 创建一个pg2的管理空间
    pg2_namespace = pg2_namespace(name)
    :ok = :pg2.create(pg2_namespace)
    # 将自己放入其中
    :ok = :pg2.join(pg2_namespace, self)
    {:ok, name}
  end
  #收到消息转发请求后，直接让本地节点进行进行广播
  def handle_info({:forward_to_local, from_pid, pool_size, topic, msg}, name) do
    # The whole broadcast will happen inside the current process
    # but only for messages coming from the distributed system.
    Local.broadcast(name, pool_size, from_pid, topic, msg)
    {:noreply, name}
  end

  defp pg2_namespace(server_name), do: {:phx, server_name}
end
