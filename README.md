# TCP bridge
TCP is a common protocol used in the internet, but tetronet didn't have an ability to carry it. Now it does B)
### How it works?
VERY simple, nothing really to explain here
<img width="402" height="911" alt="tcp bridge setup with RDP" src="https://github.com/user-attachments/assets/8bc18128-9245-4fdf-9faa-f3cbd67573be" />
p.s.
It ACTUALLY works with RDP, so if you wanted to open up gates to the tetronet, now you  can. It's working very fast
### Protocols
It uses series of packets through the tetronet along with SRTPv2 to tunnel the TCP's connection data.
### Setup process
I'm sure nobody is gonna use that thing, but there's an example of a config:\
Server config:
```
<wsProvider>
<targetIp>
<targetPort>
<verbose|unverbose>
<virtual|ciocil>
```
NOTE: if you use `virtual` modem option, you MUST specify a new RAWWS CIAS instead of the old socket.io one. All of my CIASes are using RAWWS, you can find URLs on other projects, but I recommend you to setup your own CIAS. I'm planning to cut speed for unknown people to a few Mbps so nobody will overload my dying server.
Client config:
```
<wsProvider>
<tetronetToInternetAddress>
// leave this line empty
<tcpPort>
<verbose|unverbose>
```
NOTE: This client does only support virtual modems in RAWWS mode. If you need to use LL-CIoCIL-mini, this is an open source project, you can implement it yourself ;)
### Usage examples
- RDP over tetronet
- Minecraft network game
- Videostreaming (this will be better using the UDP bridge btw)
- File transfer
- FTP
- Firmware update
- etc
### Contributing
You can add features, but you don't be sad if i don't accept it. This is a bridge which the user will trust it's trafic. So unsafe/undocumented contributions will be revoked right away.
### Forks
Every fork you make must also be safe, since how i said, the user is trusting the program with his trafic, that may be private/secret.
### Security
I won't recommend you to use it with no encryption - tetronet nodes can see all of our packets if they really want to. I don't make logs, but who knows what others do. Use Https or Ssh over it. Be wiseful about choosing the endpoint. You can be not worried about somebody connecting to a "wrong" endpoint - the endpoint is one for all and nobody can change it remotely, only by accessing the TTOI-TCP side.
### Btw
This project is older than the UDP bridge. But then it got abandoned until i made SRTPv1 and then claude code made SRTPv2, so now it works as intended and never does packet loss.
