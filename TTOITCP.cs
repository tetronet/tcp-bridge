using ModemAPI;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;

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
// 7-я строка (необязательная): через сколько минут без единого пакета из Тетронета завершить процесс,
// чтобы его перезапустил systemd/скрипт. Пусто или 0 — не завершать.
int watchdogMinutes = config.Length > 6 && int.TryParse(config[6], out int wd) ? wd : 0;
// Размер одного сообщения SRTP — максимальный. Каждое сообщение в Тетронете — отдельный кадр JSON+base64, и почти вся нагрузка
// на процессор (пробуждение потоков, разбор JSON, системные вызовы, подтверждения) растёт с числом сообщений, а не байт.
// Роутер не пропускает package_data от 87385 символов; ModemAPI больше не экранирует "+", так что base64 максимального
// сообщения всегда 80000 символов.
const int ChunkSize = SRTPClient.MaxMessageLength;

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
if (modem is VirtualModem virtualModem)
{
    // ModemAPI молча повторяет подключение каждые 2 секунды и не показывает ошибку: выводим это сами
    long reconnectStartedAt = 0;
    int reconnectAttempts = 0;
    virtualModem.OnReconnectWebsocket += delegate ()
    {
        int attempt = Interlocked.Increment(ref reconnectAttempts);
        if (attempt == 1)
        {
            reconnectStartedAt = Stopwatch.GetTimestamp();
            Console.WriteLine($"[WARNING]: [{DateTime.Now:G}] Connection to {wsProvider} lost, reconnecting...");
        }
        // примерно через 6 секунд, а потом раз в 5 минут, узнаём настоящую причину
        if (attempt == 3 || attempt % 150 == 0)
        {
            Console.WriteLine($"[WARNING]: [{DateTime.Now:G}] Still reconnecting (attempt {attempt}), checking why...");
            _ = ProbeConnectionAsync();
        }
    };
    virtualModem.OnSuccessReconnectWebsocket += delegate ()
    {
        int attempts = Interlocked.Exchange(ref reconnectAttempts, 0);
        Console.WriteLine($"[INFO]: [{DateTime.Now:G}] Reconnected to {wsProvider} after {Stopwatch.GetElapsedTime(reconnectStartedAt).TotalSeconds:F0}s ({attempts} attempts)");
    };
}

async Task ProbeConnectionAsync()
{
    using ClientWebSocket probe = new();
    using CancellationTokenSource cts = new(TimeSpan.FromSeconds(15));
    try
    {
        await probe.ConnectAsync(new Uri(wsProvider), cts.Token);
        Console.WriteLine($"[INFO]: Test connection to {wsProvider} succeeded, the server is reachable");
        probe.Abort();
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[ERROR]: Test connection to {wsProvider} failed:");
        for (Exception? e = ex; e != null; e = e.InnerException)
        {
            Console.WriteLine($"         {e.GetType().Name}: {e.Message}");
        }
    }
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
// Environment.TickCount64, а не Stopwatch: обновляется на каждый принятый пакет, а точность до миллисекунды тут не нужна
long lastPacketTimestamp = Environment.TickCount64;

modem.AttachReceiveEventNoUnfragment(delegate (Packet received, Action k)
{
    // Этот обработчик вызывается прямо из цикла приёма модема: здесь нельзя ничего блокировать.
    Volatile.Write(ref lastPacketTimestamp, Environment.TickCount64);
    if (verbose)
    {
        DebugOutput($"[INFO]: Type: {received.QueryType}");
        DebugOutput($"        Length: {received.DataBytes.Length}");
        DebugOutput($"        ConnectionID: {received.ConnectionID}");
    }

    if (received.QueryType == "ping")
    {
        ushort ttl = received.RoutersToPass;
        _ = Task.Run(delegate ()
        {
            try
            {
                // Transmit different data in a ping packet to pevent network from caching the request
                byte[] pingData = new byte[Random.Shared.Next(1, 30000)];
                Random.Shared.NextBytes(pingData);
                modem.Transmit(pingData, received.Transmitter, "pong", received.ConnectionID, null, 30000);
                Console.WriteLine($"[EVENT]: Response for client's ping packet (ping connection id is {received.ConnectionID}, TTL is {ttl})");
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
        byte[] buffer = new byte[ChunkSize];
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
            bytesRead += ReadAvailable(conn.Tcp, buffer, bytesRead);
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

// Дочитывает то, что уже пришло в сокет, не дожидаясь новых данных: одно сообщение SRTP вместо нескольких мелких
// при той же задержке. Конец потока (Read вернул 0) здесь не обрабатывается — его увидит следующий Read в цикле.
static int ReadAvailable(TcpClient tcp, byte[] buffer, int offset)
{
    int added = 0;
    while (offset + added < buffer.Length && tcp.Available > 0)
    {
        int more = tcp.GetStream().Read(buffer, offset + added, buffer.Length - offset - added);
        if (more == 0)
        {
            break;
        }
        added += more;
    }
    return added;
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

// Цикл работает на главном потоке, а не в пуле: если пул потоков встанет, статус и watchdog всё равно сработают
int lastCount = -1;
string? lastAddress = modem.LocalModemAddress?.AddressValue;
long lastStatusTimestamp = Stopwatch.GetTimestamp();
while (true)
{
    Thread.Sleep(5000);
    if (connections.Count != lastCount)
    {
        lastCount = connections.Count;
        Console.WriteLine($"[STATUS]: Active TCP connections: {lastCount}");
    }

    // Вышестоящий узел может выдать другой адрес после переподключения, и клиенты перестанут нас находить
    string? address = modem.LocalModemAddress?.AddressValue;
    if (address != lastAddress)
    {
        Console.WriteLine($"[WARNING]: [{DateTime.Now:G}] Tetronet address changed from {lastAddress} to {address}, clients must use the new one");
        lastAddress = address;
        if (address != null)
        {
            try { File.WriteAllText("ci_address.txt", address); } catch { }
        }
    }

    TimeSpan silence = TimeSpan.FromMilliseconds(Environment.TickCount64 - Volatile.Read(ref lastPacketTimestamp));
    if (Stopwatch.GetElapsedTime(lastStatusTimestamp) >= TimeSpan.FromMinutes(10))
    {
        lastStatusTimestamp = Stopwatch.GetTimestamp();
        Console.WriteLine($"[STATUS]: [{DateTime.Now:G}] connected={modem.IsModemConnected}, address={address}, " +
            $"last packet {silence.TotalSeconds:F0}s ago, connections={connections.Count}, " +
            $"pool threads={ThreadPool.ThreadCount}, queued work={ThreadPool.PendingWorkItemCount}, " +
            $"memory={Environment.WorkingSet / 1048576}MB");
    }

    if (watchdogMinutes > 0 && silence > TimeSpan.FromMinutes(watchdogMinutes))
    {
        Console.WriteLine($"[ERROR]: [{DateTime.Now:G}] No packets from tetronet for {silence.TotalMinutes:F0} minutes " +
            $"(connected={modem.IsModemConnected}, queued work={ThreadPool.PendingWorkItemCount}), exiting so the bridge can be restarted");
        Environment.Exit(3);
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
