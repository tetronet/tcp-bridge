using ModemAPI;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

string[] config = File.ReadAllLines("config.txt");
bool verbose = config.Length > 4 && config[4] == "verbose";

// ModemAPI делает много синхронных ожиданий (SendAsync().Wait(), Thread.Sleep в SRTPClient.Transmit),
// поэтому даём пулу потоков запас, чтобы он не "задыхался" при нескольких активных соединениях.
ThreadPool.GetMinThreads(out int minWorker, out int minIo);
ThreadPool.SetMinThreads(Math.Max(minWorker, 64), Math.Max(minIo, 64));

VirtualModem modem = new(config[0], new(), rawWs: true);
modem.Dial();
Console.Write("Connecting");
while (!modem.IsModemConnected)
{
    Console.Write(".");
    Thread.Sleep(100);
}
Console.WriteLine("OK");
Console.WriteLine("Connected to the Tetronet with address " + (modem.LocalModemAddress ?? throw new NullAddressException()).AddressValue);
void DebugOutput(object d)
{
    if (verbose)
    {
        Console.WriteLine(d);
    }
}

Address gateway = new(config[1]);

TcpListener listen = new(IPAddress.Any, int.Parse(config[3]));
listen.Start();
Console.WriteLine("Listening on port " + int.Parse(config[3]));

// Активные соединения: ConnectionID -> TCP-клиент + SRTP-клиент
ConcurrentDictionary<uint, ClientConnection> connections = new();

uint currentPing = 0;

_ = Task.Run(async delegate ()
{
    while (true)
    {
        try
        {
            modem.Transmit([0x00], gateway, "ping", currentPing);
            Console.WriteLine("Ping sent with Connection ID of " + currentPing);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to transmit ping #{currentPing}: {ex.Message}");
        }
        currentPing++;
        await Task.Delay(10000);
    }
});

modem.AttachReceiveEventNoUnfragment(delegate (Packet p, Action k)
{
    // Этот обработчик вызывается прямо из цикла приёма модема: здесь нельзя ничего блокировать.
    if (p.QueryType == "pong")
    {
        Console.WriteLine($"Pong received for Connection ID {p.ConnectionID}");
    }
    else if (p.QueryType == "tcp_reset" && p.Transmitter.AddressValue == gateway.AddressValue)
    {
        if (connections.TryGetValue(p.ConnectionID, out ClientConnection? conn))
        {
            Console.WriteLine($"Received TCP_RESET for connection {p.ConnectionID}");
            CloseConnection(conn, notifyRemote: false);
        }
        else
        {
            DebugOutput($"Received TCP_RESET for unknown connection {p.ConnectionID}");
        }
    }
});

void CloseConnection(ClientConnection conn, bool notifyRemote)
{
    // Закрываем ровно один раз, даже если сюда одновременно пришли несколько потоков
    if (Interlocked.Exchange(ref conn.Closed, 1) == 1)
    {
        return;
    }
    connections.TryRemove(new KeyValuePair<uint, ClientConnection>(conn.Id, conn));
    // Shutdown перед Close: уже записанные в сокет данные успеют уйти локальному клиенту
    try { conn.Tcp.Client.Shutdown(SocketShutdown.Send); } catch { }
    try { conn.Tcp.Close(); } catch { }
    try { conn.Srtp?.Close(); } catch { }
    if (notifyRemote)
    {
        try
        {
            modem.LowLevelTransmit([0x00], gateway, "tcp_reset", conn.Id, null);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to send tcp_reset for {conn.Id}: {ex.Message}");
        }
    }
    Console.WriteLine($"Connection {conn.Id} closed");
}

void Handler(SRTPClient sender, byte[] dataBytes)
{
    if (connections.TryGetValue(sender.EndPointConnectionID, out ClientConnection? conn))
    {
        conn.GotResponse = true;
        try
        {
            conn.Tcp.GetStream().Write(dataBytes, 0, dataBytes.Length);
            if (verbose) DebugOutput($"Sent {dataBytes.Length} bytes to client for connection {conn.Id}");
        }
        catch (Exception ex)
        {
            // Исключение отсюда нельзя выпускать в SRTPClient: он перестанет выдавать следующие пакеты
            Console.WriteLine($"Error sending to client {conn.Id}: {ex.Message}");
            CloseConnection(conn, notifyRemote: true);
        }
    }
    else
    {
        if (verbose) DebugOutput($"Failed to forward {dataBytes.Length} bytes");
        try
        {
            modem.LowLevelTransmit([0x00], gateway, "tcp_reset", sender.EndPointConnectionID, null);
        }
        catch { }
    }
}

// Обработчик входящих TCP-соединений
Thread acceptThread = new(() =>
{
    while (true)
    {
        try
        {
            TcpClient client = listen.AcceptTcpClient();
            ClientConnection conn;
            do
            {
                conn = new((uint)Random.Shared.NextInt64(1_000_000, uint.MaxValue), client);
            }
            while (!connections.TryAdd(conn.Id, conn));

            Console.WriteLine($"INCOMING CONNECTION! Assigned ID: {conn.Id}");

            // У каждого соединения свой поток: SRTPClient.Transmit блокирует поток (Thread.Sleep),
            // и в пуле потоков это приводило бы к его истощению и зависаниям.
            new Thread(() => HandleClient(conn)) { IsBackground = true, Name = $"tcp-{conn.Id}" }.Start();
        }
        catch (Exception ex)
        {
            Console.WriteLine("Accept error: " + ex.Message);
            Thread.Sleep(100);
        }
    }
})
{ IsBackground = true, Name = "accept" };
acceptThread.Start();

void HandleClient(ClientConnection conn)
{
    try
    {
        conn.Tcp.NoDelay = true;
        conn.Tcp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
        // Если локальный клиент перестал читать, запись не должна висеть вечно
        conn.Tcp.SendTimeout = 30000;

        SRTPClient tetronetClient = new(modem, gateway, "tcp", conn.Id, 20000000);
        conn.Srtp = tetronetClient;
        tetronetClient.OnMessageReceived += Handler;
        tetronetClient.OnAckReceived += _ => conn.GotResponse = true;
        // SRTPClient сам закрывается после слишком большого числа перепосылок
        tetronetClient.OnClose += _ => CloseConnection(conn, notifyRemote: true);
        if (conn.Closed == 1)
        {
            tetronetClient.Close();
            return;
        }

        modem.Transmit([0x00], gateway, "tcp_connect", conn.Id);
        // tcp_connect передаётся без подтверждения и может потеряться: повторяем, пока шлюз не ответит.
        // Шлюз игнорирует дубликаты tcp_connect для уже существующего соединения.
        _ = Task.Run(async () =>
        {
            for (int i = 0; i < 5; i++)
            {
                await Task.Delay(2000);
                if (conn.GotResponse || conn.Closed == 1)
                {
                    return;
                }
                try
                {
                    modem.Transmit([0x00], gateway, "tcp_connect", conn.Id);
                    DebugOutput($"Retransmitted tcp_connect for {conn.Id}");
                }
                catch { }
            }
        });

        NetworkStream stream = conn.Tcp.GetStream();
        // Крупные пакеты: каждый пакет в Тетронете — отдельное JSON+base64 сообщение со своими накладными расходами
        byte[] buffer = new byte[32768];
        while (conn.Closed == 0)
        {
            int bytesRead = stream.Read(buffer, 0, buffer.Length);
            if (bytesRead == 0)
            {
                Console.WriteLine($"Client closed connection {conn.Id}");
                // Дожидаемся, пока шлюз подтвердит (то есть доставит) всё отправленное, иначе хвост данных потеряется
                tetronetClient.WaitForAcknowledgements();
                break;
            }
            tetronetClient.Transmit(buffer[..bytesRead]);
            if (verbose) DebugOutput($"Transmitted {bytesRead} bytes to Tetronet (conn {conn.Id})");
        }
    }
    catch (Exception ex)
    {
        if (conn.Closed == 0)
        {
            Console.WriteLine($"Transmit error for {conn.Id}: {ex.Message}");
        }
    }
    CloseConnection(conn, notifyRemote: true);
}

// Основной цикл для мониторинга
int lastCount = -1;
while (true)
{
    await Task.Delay(5000);
    if (connections.Count != lastCount)
    {
        lastCount = connections.Count;
        Console.WriteLine($"Active connections: {lastCount}");
    }
}

class ClientConnection(uint id, TcpClient tcp)
{
    public readonly uint Id = id;
    public readonly TcpClient Tcp = tcp;
    public SRTPClient? Srtp;
    public int Closed;
    public volatile bool GotResponse;
}
