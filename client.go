package steam

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/0xAozora/go-steam/cryptoutil"
	"github.com/0xAozora/go-steam/netutil"
	"github.com/0xAozora/go-steam/protocol"
	"github.com/0xAozora/go-steam/protocol/protobuf"
	"github.com/0xAozora/go-steam/protocol/steamlang"
	"github.com/0xAozora/go-steam/steamid"
	"golang.org/x/net/proxy"
)

// Represents a client to the Steam network.
// Always poll events from the channel returned by Events() or receiving messages will stop.
// All access, unless otherwise noted, should be threadsafe.
//
// When a FatalErrorEvent is emitted, the connection is automatically closed. The same client can be used to reconnect.
// Other errors don't have any effect.
type Client struct {
	// these need to be 64 bit aligned for sync/atomic on 32bit
	sessionId    int32
	_            uint32
	steamId      uint64
	currentJobId uint64

	Auth          *Auth
	Social        *Social
	Web           *Web
	Notifications *Notifications
	Trading       *Trading
	GC            *GameCoordinator

	events        chan interface{}
	handlers      []PacketHandler
	handlersMutex sync.RWMutex
	JobHandlers   map[uint64]func(*protocol.Packet) error
	JobMutex      sync.Mutex

	tempSessionKey []byte

	ConnectionTimeout time.Duration

	mutex     sync.RWMutex // guarding conn and writeChan
	Conn      connection
	writeChan chan protocol.IMsg
	writeBuf  *bytes.Buffer
	heartbeat *time.Ticker

	Proxy  proxy.ContextDialer
	manual bool
}

type PacketHandler interface {
	HandlePacket(*protocol.Packet)
}

// Do not use this
// Call Read / WriteLoop explicitelly if needed
func NewManualClient() *Client {
	return &Client{
		events:      make(chan interface{}, 3),
		writeBuf:    new(bytes.Buffer),
		JobHandlers: make(map[uint64]func(*protocol.Packet) error),
		manual:      true,
	}
}

func NewClient() *Client {
	client := &Client{
		events:      make(chan interface{}, 3),
		writeBuf:    new(bytes.Buffer),
		JobHandlers: make(map[uint64]func(*protocol.Packet) error),
	}

	client.AddDefaultHandlers()

	return client
}

func (c *Client) AddDefaultHandlers() {
	c.Auth = &Auth{Client: c}
	c.RegisterPacketHandler(c.Auth) // Comment Out

	c.Social = NewSocial(c)
	c.RegisterPacketHandler(c.Social)

	c.Web = &Web{Client: c}
	c.RegisterPacketHandler(c.Web)

	c.Notifications = NewNotifications(c)
	c.RegisterPacketHandler(c.Notifications)

	c.Trading = &Trading{Client: c}
	c.RegisterPacketHandler(c.Trading)

	c.GC = NewGC(c)
	c.RegisterPacketHandler(c.GC)
}

// Get the event channel. By convention all events are pointers, except for errors.
// It is never closed.
func (c *Client) Events() <-chan interface{} {
	return c.events
}

func (c *Client) Emit(event interface{}) {
	if !c.manual {
		c.events <- event
	}
}

// Emits a FatalErrorEvent formatted with fmt.Errorf and disconnects.
func (c *Client) Fatalf(format string, a ...interface{}) {
	c.Emit(FatalErrorEvent(fmt.Errorf(format, a...)))
	c.Disconnect()
}

// Emits an error formatted with fmt.Errorf.
func (c *Client) Errorf(format string, a ...interface{}) {
	c.Emit(fmt.Errorf(format, a...))
}

// Registers a PacketHandler that receives all incoming packets.
func (c *Client) RegisterPacketHandler(handler PacketHandler) {
	c.handlersMutex.Lock()
	c.handlers = append(c.handlers, handler)
	c.handlersMutex.Unlock()
}

func (c *Client) GetNextJobId() protocol.JobId {
	return protocol.JobId(atomic.AddUint64(&c.currentJobId, 1))
}

func (c *Client) SteamId() steamid.SteamId {
	return steamid.SteamId(atomic.LoadUint64(&c.steamId))
}

func (c *Client) SessionId() int32 {
	return atomic.LoadInt32(&c.sessionId)
}

func (c *Client) Connected() (ok bool) {
	c.mutex.RLock()
	ok = c.Conn != nil
	c.mutex.RUnlock()
	return
}

// Connects to a random Steam server and returns its address.
// If this client is already connected, it is disconnected first.
// This method tries to use an address from the Steam Directory and falls
// back to the built-in server list if the Steam Directory can't be reached.
// If you want to connect to a specific server, use `ConnectTo`.
func (c *Client) Connect(ctx context.Context) (*netutil.PortAddr, error) {
	var server *netutil.PortAddr

	// try to initialize the directory cache
	if !steamDirectoryCache.IsInitialized() {
		_ = steamDirectoryCache.Initialize()
	}
	if steamDirectoryCache.IsInitialized() {
		server = steamDirectoryCache.GetRandomCM()
	} else {
		server = GetRandomCM()
	}

	err := c.ConnectTo(server, ctx)
	return server, err
}

// Connects to a specific server.
// You may want to use one of the `GetRandom*CM()` functions in this package.
// If this client is already connected, it is disconnected first.
func (c *Client) ConnectTo(addr *netutil.PortAddr, ctx context.Context) error {
	return c.ConnectToBind(addr, nil, ctx)
}

// Connects to a specific server, and binds to a specified local IP
// If this client is already connected, it is disconnected first.
func (c *Client) ConnectToBind(addr *netutil.PortAddr, local *net.TCPAddr, ctx context.Context) error {
	c.Disconnect()

	conn, err := dialTCP(local, addr.ToTCPAddr(), ctx, c.Proxy)
	if err != nil {
		c.Fatalf("Connect failed: %v", err)
		return err
	}
	c.Conn = conn
	c.writeChan = make(chan protocol.IMsg, 5)

	if !c.manual {
		go c.ReadLoop()
		go c.WriteLoop()
	}

	return nil
}

func (c *Client) Disconnect() {
	c.mutex.Lock()

	if c.Conn == nil {
		c.mutex.Unlock()
		return
	}

	c.Conn.Close()
	c.Conn = nil
	if c.heartbeat != nil {
		c.heartbeat.Stop()
	}
	close(c.writeChan)
	// c.Emit(&DisconnectedEvent{})
	c.mutex.Unlock()
}

// Adds a message to the send queue. Modifications to the given message after
// writing are not allowed (possible race conditions).
//
// Writes to this client when not connected are ignored.
func (c *Client) Send(msg protocol.IMsg) {
	c.mutex.Lock()
	if c.Conn == nil {
		c.mutex.Unlock()
		return
	}
	c.writeChan <- msg
	c.mutex.Unlock()
}

func (c *Client) Write(msg protocol.IMsg) error {

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if c.Conn == nil {
		return nil
	}

	if cm, ok := msg.(protocol.IClientMsg); ok {
		cm.SetSessionId(c.SessionId())
		cm.SetSteamId(c.SteamId())
	}

	// Serialize
	err := msg.Serialize(c.writeBuf)
	if err != nil {
		c.writeBuf.Reset()
		return fmt.Errorf("Error serializing message %v: %v", msg, err)
	}

	// Write
	err = c.Conn.Write(c.writeBuf.Bytes())
	c.writeBuf.Reset()
	if err != nil {
		return fmt.Errorf("Error writing message %v: %v", msg, err)
	}

	return nil
}

func (c *Client) Read() (*protocol.Packet, error) {
	return c.Conn.Read()
}

func (c *Client) ReadLoop() {
	for {
		// This *should* be atomic on most platforms, but the Go spec doesn't guarantee it
		c.mutex.RLock()
		conn := c.Conn
		c.mutex.RUnlock()
		if conn == nil {
			return
		}
		packet, err := conn.Read()

		if err != nil {
			c.Fatalf("Error reading from the connection: %v", err)
			return
		}
		c.handlePacket(packet)
	}
}

func (c *Client) WriteLoop() {
	for {
		msg, ok := <-c.writeChan
		if !ok {
			return
		}

		if err := c.Write(msg); err != nil {
			c.Fatalf(err.Error())
		}
	}
}

func (c *Client) heartbeatLoop(seconds time.Duration) {
	if c.heartbeat != nil {
		c.heartbeat.Stop()
	}
	c.heartbeat = time.NewTicker(seconds * time.Second)
	for {
		_, ok := <-c.heartbeat.C
		if !ok {
			break
		}
		c.Send(protocol.NewClientMsgProtobuf(steamlang.EMsg_ClientHeartBeat, new(protobuf.CMsgClientHeartBeat)))
	}
	c.heartbeat = nil
}

func (c *Client) handlePacket(packet *protocol.Packet) {

	// Check Job Response
	c.JobMutex.Lock()
	fn := c.JobHandlers[uint64(packet.TargetJobId)]
	if fn != nil {
		delete(c.JobHandlers, uint64(packet.TargetJobId))
		c.JobMutex.Unlock()

		if err := fn(packet); err != nil {
			c.Fatalf(err.Error())
		}
		return
	} else {
		c.JobMutex.Unlock()
	}

	switch packet.EMsg {
	case steamlang.EMsg_ChannelEncryptRequest:
		if err := c.HandleChannelEncryptRequest(packet); err != nil {
			c.Fatalf(err.Error())
		}
	case steamlang.EMsg_ChannelEncryptResult:
		if err := c.HandleChannelEncryptResult(packet); err != nil {
			c.Fatalf(err.Error())
		} else {
			c.Emit(&ConnectedEvent{})
		}
	case steamlang.EMsg_Multi:
		packets, err := c.HandleMulti(packet)
		if err != nil {
			c.Errorf(err.Error())
		}
		for _, packet := range packets {
			c.handlePacket(packet)
		}
	case steamlang.EMsg_ClientCMList:
		c.Emit(c.HandleClientCMList(packet))
	}

	c.handlersMutex.RLock()
	for _, handler := range c.handlers {
		handler.HandlePacket(packet)
	}
	c.handlersMutex.RUnlock()
}

func (c *Client) HandleChannelEncryptRequest(packet *protocol.Packet) error {
	body := steamlang.NewMsgChannelEncryptRequest()
	packet.ReadMsg(body)

	if body.Universe != steamlang.EUniverse_Public {
		return fmt.Errorf("Invalid univserse %v!", body.Universe)
	}

	c.tempSessionKey = make([]byte, 32)
	rand.Read(c.tempSessionKey)
	encryptedKey := cryptoutil.RSAEncrypt(GetPublicKey(steamlang.EUniverse_Public), c.tempSessionKey)

	payload := new(bytes.Buffer)
	payload.Write(encryptedKey)
	binary.Write(payload, binary.LittleEndian, crc32.ChecksumIEEE(encryptedKey))
	payload.WriteByte(0)
	payload.WriteByte(0)
	payload.WriteByte(0)
	payload.WriteByte(0)

	c.Send(protocol.NewMsg(steamlang.NewMsgChannelEncryptResponse(), payload.Bytes()))

	return nil
}

func (c *Client) HandleChannelEncryptResult(packet *protocol.Packet) error {
	body := steamlang.NewMsgChannelEncryptResult()
	packet.ReadMsg(body)

	if body.Result != steamlang.EResult_OK {
		return fmt.Errorf("Encryption failed: %v", body.Result)
	}
	c.Conn.SetEncryptionKey(c.tempSessionKey)
	c.tempSessionKey = nil

	return nil
}

func (c *Client) HandleMulti(packet *protocol.Packet) ([]*protocol.Packet, error) {
	body := new(protobuf.CMsgMulti)
	packet.ReadProtoMsg(body)

	payload := body.GetMessageBody()

	if body.GetSizeUnzipped() > 0 {
		r, err := gzip.NewReader(bytes.NewReader(payload))
		if err != nil {
			return nil, fmt.Errorf("handleMulti: Error while decompressing: %v", err)
		}

		payload, err = io.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("handleMulti: Error while decompressing: %v", err)
		}
	}

	pr := bytes.NewReader(payload)
	var packets []*protocol.Packet
	for pr.Len() > 0 {
		var length uint32
		binary.Read(pr, binary.LittleEndian, &length)
		packetData := make([]byte, length)
		pr.Read(packetData)
		p, err := protocol.NewPacket(packetData)
		if err != nil {
			//c.Errorf("Error reading packet in Multi msg %v: %v", packet, err)
			continue
		}
		packets = append(packets, p)
	}
	return packets, nil
}

func (c *Client) HandleClientCMList(packet *protocol.Packet) *ClientCMListEvent {
	body := new(protobuf.CMsgClientCMList)
	packet.ReadProtoMsg(body)

	l := make([]*netutil.PortAddr, 0)
	for i, ip := range body.GetCmAddresses() {
		l = append(l, &netutil.PortAddr{
			IP:   readIp(ip),
			Port: uint16(body.GetCmPorts()[i]),
		})
	}

	return &ClientCMListEvent{l}
}

func readIp(ip uint32) net.IP {
	r := make(net.IP, 4)
	r[3] = byte(ip)
	r[2] = byte(ip >> 8)
	r[1] = byte(ip >> 16)
	r[0] = byte(ip >> 24)
	return r
}
