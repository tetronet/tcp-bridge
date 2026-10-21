using ModemAPI;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

Console.WriteLine("TCP-Bridge v1.1 (Optimized for Tetronet)");

string[] config = File.ReadAllLines("config.txt");

Address? wantedAddress = null;
if (File.Exists("ci_address.txt"))
{
    wantedAddress = new(File.ReadAllText("ci_address.txt"));
}
if (config[0] != "server")
{
    Console.WriteLine("Server config parsing error: terminating");
    Thread.Sleep(5000);
    throw new FormatException("config line 1 not \"server\"");
}
string wsProvider = config[1];
IPAddress targetAddress = IPAddress.Parse(config[2]);
int targetPort = int.Parse(config[3]);
bool verbose = config[4] == "verbose";

void DebugOutput(object d)
{
    if (verbose)
    {
        Console.WriteLine(d);
    }
}

// ModemAPI делает много синхронных ожиданий (SendAsync().Wait(), Thread.Sleep в SRTPClient.Transmit),
// поэтому даём пулу потоков запас, чтобы он не "задыхался" при нескольких активных соединениях.
ThreadPool.GetMinThreads(out int minWorker, out int minIo);
ThreadPool.SetMinThreads(Math.Max(minWorker, 64), Math.Max(minIo, 64));

IModem modem;
if (config[5] == "virtual")
{
    modem = new VirtualModem(wsProvider, new(wantedAddress ?? new(), true, null), rawWs: true);
}
else if (config[5] == "ciocil")
{
    LowLatencyPhysicalModem mdm = new(wsProvider.Split(' ')[0], int.Parse(wsProvider.Split(' ')[1]), L1Types.L1_SERIAL, true);
    mdm.AttachCRCMismatchEvent(delegate ()
    {
        Console.WriteLine("[WARNING]: CRC-32 Mismatched on the Level One");
    });
    mdm.L1DroppedData += delegate (L1DropBytesReasons reason, int count)
    {
        Console.WriteLine($"[WARNING]: Level One dropped {count} bytes, reason is {reason}");
    };
    modem = mdm;
}
else
{
    throw new FormatException("sixth line of the config must be \"virtual\" for tetronet over socket.io or \"ciocil\" for tetronet over serial (LL-CIoCIL-mini)");
}
modem.Dial();
Console.Write("[PROCESS]: Connecting");
while (!modem.IsModemConnected)
{
    Console.Write(".");
    Thread.Sleep(100);
}
Console.WriteLine("OK");
Console.WriteLine("[INFO]: Connected to tetronet with address " + (modem.LocalModemAddress ?? throw new NullAddressException()).AddressValue);
File.WriteAllText("ci_address.txt", modem.LocalModemAddress.AddressValue);

// ConnectionID -> TCP-клиент + SRTP-клиент + адрес в Тетронете
ConcurrentDictionary<uint, BridgeConnection> connections = new();

modem.AttachReceiveEventNoUnfragment(delegate (Packet received, Action k)
{
    // Этот обработчик вызывается прямо из цикла приёма модема: здесь нельзя ничего блокировать.
    if (verbose)
    {
        DebugOutput($"[INFO]: Type: {received.QueryType}");
        DebugOutput($"        Length: {received.DataBytes.Length}");
        DebugOutput($"        ConnectionID: {received.ConnectionID}");
    }

    if (received.QueryType == "ping")
    {
        _ = Task.Run(delegate ()
        {
            try
            {
                // Transmit different data in a ping packet to pevent network from caching the request
                byte[] pingData = new byte[Random.Shared.Next(1, 30000)];
                Random.Shared.NextBytes(pingData);
                modem.Transmit(pingData, received.Transmitter, "pong", received.ConnectionID, null, 30000);
                Console.WriteLine($"[EVENT]: Response for client's ping packet (ping connection id is {received.ConnectionID})");
            }
            catch
            {
                DebugOutput("[ERROR]: Failed to answer to ping request");
            }
        });
    }
    else if (received.QueryType == "tcp_connect")
    {
        BridgeConnection conn = new(received.ConnectionID, received.Transmitter, new TcpClient());
        // TryAdd атомарен: повторный (или продублированный сетью) tcp_connect не создаст второе соединение
        if (!connections.TryAdd(conn.Id, conn))
        {
            conn.Tcp.Dispose();
            DebugOutput("[INFO]: Duplicate tcp_connect ignored for connection " + received.ConnectionID);
            return;
        }
        Console.WriteLine("[EVENT]: Received tcp_connect packet for connection " + conn.Id);
        new Thread(() => RunConnection(conn)) { IsBackground = true, Name = $"tcp-{conn.Id}" }.Start();
    }
    else if (received.QueryType == "tcp_reset")
    {
        if (connections.TryGetValue(received.ConnectionID, out BridgeConnection? conn) && conn.Peer.Equals(received.Transmitter))
        {
            Console.WriteLine("[INFO]: TCP client was closed (TCP RESET) " + conn.Id);
            CloseConnection(conn, notifyRemote: false);
        }
    }
});

void RunConnection(BridgeConnection conn)
{
    try
    {
        Console.WriteLine($"[INFO]: Creating TCP client {conn.Id}, target {targetAddress}:{targetPort}");
        conn.Tcp.NoDelay = true;
        conn.Tcp.SendTimeout = 30000;
        using (CancellationTokenSource cts = new(TimeSpan.FromSeconds(15)))
        {
            conn.Tcp.ConnectAsync(targetAddress, targetPort, cts.Token).AsTask().GetAwaiter().GetResult();
        }
        conn.Tcp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
        Console.WriteLine("[EVENT]: TCP client connected " + conn.Id);

        SRTPClient srtp = new(modem, conn.Peer, "tcp", conn.Id, 20000000);
        conn.Srtp = srtp;
        srtp.OnMessageReceived = FromTetronetToTcpSocket;
        // SRTPClient сам закрывается после слишком большого числа перепосылок
        srtp.OnClose += _ => CloseConnection(conn, notifyRemote: true);
        if (conn.Closed == 1)
        {
            // tcp_reset пришёл, пока мы подключались
            srtp.Close();
            return;
        }

        NetworkStream stream = conn.Tcp.GetStream();
        // Крупные пакеты: каждый пакет в Тетронете — отдельное JSON+base64 сообщение со своими накладными расходами
        byte[] buffer = new byte[32768];
        while (conn.Closed == 0)
        {
            int bytesRead = stream.Read(buffer, 0, buffer.Length);
            if (bytesRead == 0)
            {
                // 0 байт = удалённая сторона закрыла соединение
                Console.WriteLine("[EVENT]: TCP connection closed by the target " + conn.Id);
                // Дожидаемся, пока клиент подтвердит (то есть доставит) всё отправленное, иначе хвост данных потеряется
                srtp.WaitForAcknowledgements();
                break;
            }
            if (verbose) DebugOutput("[PROCESS]: Transmitting TCP segment...");
            srtp.Transmit(buffer[..bytesRead]);
            if (verbose) DebugOutput($"[EVENT]: Transmitted TCP segment of {bytesRead} bytes from {conn.Id} to Tetronet");
        }
    }
    catch (Exception ex)
    {
        if (conn.Closed == 0)
        {
            Console.WriteLine($"[ERROR]: TCP connection {conn.Id} failed: {ex.Message}");
        }
    }
    CloseConnection(conn, notifyRemote: true);
}

void FromTetronetToTcpSocket(SRTPClient sender, byte[] data)
{
    if (!connections.TryGetValue(sender.EndPointConnectionID, out BridgeConnection? conn))
    {
        Console.WriteLine("[ERROR]: Failed to forward: unknown connection " + sender.EndPointConnectionID);
        return;
    }
    try
    {
        if (verbose) DebugOutput("[PROCESS]: Transmissing TCP segment (backdir)...");
        conn.Tcp.GetStream().Write(data, 0, data.Length);
        if (verbose) DebugOutput($"[EVENT]: Transmitted Tetronet packet of {data.Length} bytes from {conn.Id} to TCP socket");
    }
    catch (Exception ex)
    {
        // Исключение отсюда нельзя выпускать в SRTPClient: он перестанет выдавать следующие пакеты
        Console.WriteLine($"[ERROR]: Writing to TCP {conn.Id} failed: {ex.Message}");
        CloseConnection(conn, notifyRemote: true);
    }
}

void CloseConnection(BridgeConnection conn, bool notifyRemote)
{
    // Закрываем ровно один раз, даже если сюда одновременно пришли несколько потоков
    if (Interlocked.Exchange(ref conn.Closed, 1) == 1)
    {
        return;
    }
    connections.TryRemove(new KeyValuePair<uint, BridgeConnection>(conn.Id, conn));
    // Shutdown перед Close: уже записанные в сокет данные успеют уйти серверу назначения
    try { conn.Tcp.Client.Shutdown(SocketShutdown.Send); } catch { }
    try { conn.Tcp.Close(); } catch { }
    try { conn.Srtp?.Close(); } catch { }

    if (notifyRemote)
    {
        try
        {
            modem.LowLevelTransmit([0x00], conn.Peer, "tcp_reset", conn.Id);
            Console.WriteLine($"[INFO]: Transmitted tcp_reset for {conn.Id} to {conn.Peer.AddressValue}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR]: Failed to send tcp_reset: {ex.Message}");
        }
    }
    Console.WriteLine($"[INFO]: Connection {conn.Id} cleaned up");
}

int lastCount = -1;
while (true)
{
    await Task.Delay(5000);
    if (connections.Count != lastCount)
    {
        lastCount = connections.Count;
        Console.WriteLine($"[STATUS]: Active TCP connections: {lastCount}");
    }
}

class BridgeConnection(uint id, Address peer, TcpClient tcp)
{
    public readonly uint Id = id;
    public readonly Address Peer = peer;
    public readonly TcpClient Tcp = tcp;
    public SRTPClient? Srtp;
    public int Closed;
}
