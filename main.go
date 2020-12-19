// MIT License Copyright(c) 2020 Hiroshi Shimamoto
// vim: set sw=4 sts=4:
package main
import (
    "encoding/binary"
    "fmt"
    "io"
    "math/rand"
    "net"
    "os"
    "time"

    log "github.com/sirupsen/logrus"
    "github.com/hshimamoto/go-session"
)

const MSS int = 1280
const RDWRSZ int = 4096

type StreamBuffer struct {
    buf []byte
    sz int
    rptr, wptr int
}

func NewStreamBuffer(sz int) *StreamBuffer {
    b := &StreamBuffer{}
    b.buf = make([]byte, sz)
    b.sz = sz
    b.rptr = 0
    b.wptr = 0
    return b
}

type Stream struct {
    sid int
    used bool
    key int
    renc, wenc byte
    in, out *StreamBuffer
    running bool
    mq chan *Message
    bell chan bool // doorbell
    sendq chan []byte
    recvq chan []byte
    established bool
    recv []byte
}

func NewStream(sid, sz int) *Stream {
    s := &Stream{}
    s.sid = sid
    s.in = NewStreamBuffer(sz)
    s.out = NewStreamBuffer(sz)
    s.used = false
    s.Init(rand.Intn(65536))
    return s
}

func (s *Stream)Init(key int) {
    s.running = false
    s.established = false
    s.key = key
    s.renc = byte(key)
    s.wenc = byte(key)
    s.recv = nil
    s.mq = make(chan *Message, 32)
    s.sendq = make(chan []byte, 32)
    s.recvq = make(chan []byte, 32)
    s.bell = make(chan bool, 32)
}

func (s *Stream)Tracef(fmt string, a ...interface{}) {
    args := make([]interface{}, len(a) + 1)
    args[0] = s.sid
    copy(args[1:], a)
    log.Tracef("[%d]" + fmt, args...)
}

func (s *Stream)Logf(fmt string, a ...interface{}) {
    args := make([]interface{}, len(a) + 1)
    args[0] = s.sid
    copy(args[1:], a)
    log.Infof("[%d]" + fmt, args...)
}

func (s *Stream)Runner(queue chan<- []byte) {
    // uplink buffer
    ulbuf := []byte(nil)
    pendingbuf := []byte(nil)
    buflen := 0
    ulptr := 0
    ulack := 0
    ulseq := 0
    ackseq := 0
    lastseq := 0
    ackflag := false
    dlseq := 0
    ultime := time.Now()
    ackq := make(chan bool, 32)
    ticker := time.NewTicker(time.Second)
    lastrecv := time.Now().Add(time.Minute)
    for s.running {
	if pendingbuf == nil || len(pendingbuf) < 32768 {
	    select {
	    case next := <-s.sendq:
		s.Tracef("dequeue %d bytes (current %d, pending %d)\n", len(next), buflen, len(pendingbuf))
		if pendingbuf != nil {
		    pendingbuf = append(pendingbuf, next...)
		} else {
		    pendingbuf = next
		}
	    default:
	    }
	}
	if pendingbuf != nil {
	    if ulack == lastseq {
		ulbuf = pendingbuf
		buflen = len(ulbuf)
		ulptr = ulseq
		lastseq = (ulseq + buflen) % 65536
		pendingbuf = nil
		s.Tracef("replace buffer lastseq=%d (prev %d)\n", lastseq, ulack)
	    }
	}
	offset := 0
	if ulptr != lastseq {
	    ultime = time.Now().Add(500 * time.Millisecond)
	}
	for ulptr != lastseq {
	    datalen := ((lastseq + 65536) - ulptr) % 65536
	    if datalen > MSS {
		datalen = MSS
	    }
	    seq0 := (ulseq + offset) % 65536
	    seq1 := (ulseq + offset + datalen) % 65536
	    msg := &Message{
		mtype: MSG_DATA,
		sid: s.sid,
		key: s.key,
		seq0: seq0,
		seq1: seq1,
	    }
	    msg.data = ulbuf[offset:offset+datalen]
	    s.Tracef("Push Data seq %d-%d\n", seq0, seq1)
	    buf := msg.Pack()
	    queue <- buf
	    ulptr = seq1
	    offset += datalen
	}
	select {
	case msg := <-s.mq:
	    lastrecv = time.Now().Add(time.Minute)
	    s.established = true
	    switch msg.mtype {
	    case MSG_DATA:
		s.Tracef("MSG: Data seq %d-%d\n", msg.seq0, msg.seq1)
		if msg.seq0 == dlseq {
		    if ackseq != msg.seq1 {
			s.Tracef("Change ackseq %d->%d\n", ackseq, msg.seq1)
		    }
		    s.recvq <- msg.data
		    s.Tracef("recvq: enqueue %d bytes\n", len(msg.data))
		    dlseq = msg.seq1
		    ackseq = msg.seq1
		}
		if ackflag == false {
		    ackq <-true
		    ackflag = true
		}
	    case MSG_ACK:
		s.Tracef("MSG: Ack seq %d-%d\n", msg.seq0, msg.seq1)
		ulack = msg.seq0
		ulseq = ulack
	    }
	case <-ticker.C:
	    // must wait a bit
	    if time.Now().After(ultime) {
		if ulseq != lastseq {
		    s.Tracef("rewind %d->%d (%d)\n", ulptr, ulseq, lastseq)
		    ulptr = ulseq
		}
	    }
	    if time.Now().After(lastrecv) {
		s.Logf("no activity\n")
		// close stream
		s.running = false
	    }
	case <-ackq:
	    // dequeue all
	    empty := false
	    for !empty {
		select {
		case <-ackq:
		default:
		    empty = true
		}
	    }
	    s.Tracef("Send Ack %d\n", ackseq)
	    // ack!
	    msg := &Message {
		mtype: MSG_ACK,
		sid: s.sid,
		key: s.key,
		seq0: ackseq,
		seq1: ackseq,
	    }
	    buf := msg.Pack()
	    queue <- buf
	    ackflag = false
	case <-s.bell:
	    // ignore
	}
    }
    go func() {
	time.Sleep(time.Minute)
	// make it's free
	s.used = false
	log.Printf("stream %d is now free\n", s.sid)
    }()
}

func (s *Stream)StartRunner(queue chan<- []byte) {
    log.Printf("start Runner %d %d\n", s.sid, s.key)
    s.running = true
    go s.Runner(queue)
}

func (s *Stream)Read(buf []byte) (int, error) {
    if s.recv == nil {
	s.recv = <-s.recvq
	s.Tracef("recvq: dequeue %d bytes\n", len(s.recv))
    }
    n := len(buf)
    if n > len(s.recv) {
	n = len(s.recv)
    }
    for i := 0; i < n; i++ {
	buf[i] = s.recv[i] ^ s.renc
	s.renc++
    }
    if n == len(s.recv) {
	s.recv = nil
    } else {
	s.recv = s.recv[n:]
	s.Tracef("rest %d bytes\n", len(s.recv))
    }
    return n, nil
}

func (s *Stream)Write(buf []byte) (int, error) {
    sendbuf := make([]byte, len(buf))
    n := len(buf)
    for i := 0; i < n; i++ {
	sendbuf[i] = buf[i] ^ s.wenc
	s.wenc++
    }
    s.sendq <- sendbuf
    s.bell <- true
    return len(buf), nil
}

type Message struct {
    mtype int
    sid int
    key int
    seq0, seq1 int
    data []byte
}

const MSG_DATA	int = 0x44 // Data
const MSG_ACK	int = 0x41 // Ack
const MSG_OPEN	int = 0x4f // Open
const MSG_RESET	int = 0x52 // Reset
const MSG_PROBE	int = 0x50 // Probe

func (m *Message)Pack() []byte {
    datalen := len(m.data)
    msglen := 1 + 1 + 2 + 2 + 2 + datalen
    buf := make([]byte, msglen)
    buf[0] = byte(m.mtype)
    buf[1] = byte(m.sid)
    binary.LittleEndian.PutUint16(buf[2:], uint16(m.key))
    binary.LittleEndian.PutUint16(buf[4:], uint16(m.seq0))
    binary.LittleEndian.PutUint16(buf[6:], uint16(m.seq1))
    copy(buf[8:], m.data)
    return buf
}

func ParseMessage(buf []byte) *Message {
    msg := &Message{}
    msg.mtype = int(buf[0])
    msg.sid = int(buf[1])
    msg.key = int(binary.LittleEndian.Uint16(buf[2:]))
    msg.seq0 = int(binary.LittleEndian.Uint16(buf[4:]))
    msg.seq1 = int(binary.LittleEndian.Uint16(buf[6:]))
    msg.data = make([]byte, len(buf[8:]))
    copy(msg.data, buf[8:])
    return msg
}

type UDPconn struct {
    addr *net.UDPAddr
    conn *net.UDPConn
    running bool
    connected bool
    queue chan []byte
    mq chan *Message
    streams []*Stream
    nr_streams int
    handler func(s *Stream, remote string)
}

func NewUDPConn(laddr, raddr string) (*UDPconn, error) {
    u := &UDPconn{}
    addr, err := net.ResolveUDPAddr("udp", raddr)
    if err != nil {
	return nil, err
    }
    u.addr = addr
    addr, err = net.ResolveUDPAddr("udp", laddr)
    if err != nil {
	return nil, err
    }
    conn, err := net.ListenUDP("udp", addr)
    if err != nil {
	return nil, err
    }
    u.conn = conn
    u.queue = make(chan []byte, 32)
    u.mq = make(chan *Message, 32)
    u.nr_streams = 64
    u.streams = make([]*Stream, u.nr_streams)
    for i, _ := range u.streams {
	u.streams[i] = NewStream(i, 65536)
    }
    u.handler = nil
    return u, err
}

func (u *UDPconn)AllocStream(n int) *Stream {
    if n < 0 {
	for _, s := range u.streams {
	    if s.used {
		continue
	    }
	    // mark it
	    s.used = true
	    return s
	}
    } else {
	if n >= u.nr_streams {
	    return nil
	}
	s := u.streams[n]
	if s.used {
	    return nil
	}
	// mark it
	s.used = true
	return s
    }
    return nil
}

func (u *UDPconn)Receiver() {
    conn := u.conn
    buf := make([]byte, 1500)
    for u.running {
	n, addr, err := conn.ReadFromUDP(buf)
	if err != nil {
	    log.Printf("Read: %v\n", err)
	    continue
	}
	if addr.String() != u.addr.String() {
	    continue
	}
	if u.connected == false {
	    log.Printf("connected with %v\n", addr)
	    u.connected = true
	    continue
	}
	//log.Printf("%d bytes from %v\n", n, addr)
	if buf[0] == 0x50 { // 'P'robe
	    // sendback probe
	    u.queue <- []byte("probe")
	    continue
	}
	if buf[0] == 0x70 { // 'p'robe
	    continue
	}
	// parse
	msg := ParseMessage(buf[:n])
	u.mq <- msg
    }
}

func (u *UDPconn)Sender() {
    ticker := time.NewTicker(10 * time.Second)
    for u.running {
	select {
	case buf := <-u.queue:
	    u.conn.WriteToUDP(buf, u.addr)
	case <-ticker.C:
	    u.conn.WriteToUDP([]byte("Probe"), u.addr)
	}
    }
}

func (u *UDPconn)Connection() {
    for u.running {
	select {
	case msg := <-u.mq:
	    sid := msg.sid
	    if sid < 0 || sid >= u.nr_streams {
		break
	    }
	    s := u.streams[sid]
	    switch msg.mtype {
	    case MSG_DATA, MSG_ACK:
		if msg.key == s.key {
		    s.mq <- msg
		}
	    case MSG_OPEN:
		log.Printf("recv OPEN %d %d\n", msg.sid, msg.key)
		// try to allocate s
		if s.used {
		    if s.key != msg.key {
			log.Printf("bad OPEN vs %d\n", s.key)
			// send back reset
			msg := &Message{
			    mtype: MSG_RESET,
			    sid: sid,
			}
			u.queue <- msg.Pack()
		    }
		    // just ignore
		    break
		}
		// start new stream
		s.Init(msg.key)
		s.used = true
		s.established = true // server side
		s.StartRunner(u.queue)
		// call handler
		if u.handler != nil {
		    remote := string(msg.data)
		    u.handler(s, remote)
		}
		ack := &Message{
		    mtype: MSG_ACK,
		    sid: sid,
		    key: s.key,
		    seq0: 0,
		    seq1: 0,
		}
		u.queue <- ack.Pack()
		u.queue <- ack.Pack()
		u.queue <- ack.Pack()
	    case MSG_RESET:
		log.Printf("recv RESET %d %d\n", msg.sid, msg.key)
		// close the stream
		s.running = false
	    }
	}
    }
}

func (u *UDPconn)Connect() {
    // start receiver
    u.running = true
    u.connected = false
    go u.Receiver()
    // connect
    cnt := 0
    for u.connected == false {
	u.conn.WriteToUDP([]byte("Probe"), u.addr)
	time.Sleep(200 * time.Millisecond)
	cnt++
	if cnt % 10 == 0 {
	    time.Sleep(time.Second)
	}
    }
    // start sender
    go u.Sender()
    // start connection
    go u.Connection()
}

func (u *UDPconn)OpenStream(remote string) *Stream {
    s := u.AllocStream(-1)
    s.Init(rand.Intn(65536))
    log.Printf("[sid:%d key:%d]try to open %s\n", s.sid, s.key, remote)
    s.StartRunner(u.queue)
    for i := 0; i < 10; i++ {
	msg := &Message{
	    mtype: MSG_OPEN,
	    sid: s.sid,
	    key: s.key,
	    seq0: len(remote),
	    seq1: len(remote),
	    data: []byte(remote),
	}
	u.queue <- msg.Pack()
	time.Sleep(100 * time.Millisecond)
	if !s.running || s.established {
	    break
	}
    }
    if !s.established {
	log.Printf("failed to open")
	return nil
    }
    return s
}

func checker(laddr string) {
    addr, err := net.ResolveUDPAddr("udp", laddr)
    if err != nil {
	log.Printf("ResolveUDPAddr: %v\n", err)
	return
    }
    conn, err := net.ListenUDP("udp", addr)
    if err != nil {
	log.Printf("ListenUDP: %v\n", err)
	return
    }
    buf := make([]byte, 1500)
    for {
	_, addr, err := conn.ReadFromUDP(buf)
	if err != nil {
	    log.Printf("ReadFromUDP: %v\n", err)
	    continue
	}
	resp := fmt.Sprintf("Probe %v", addr)
	conn.WriteToUDP([]byte(resp), addr)
	conn.WriteToUDP([]byte(resp), addr)
	conn.WriteToUDP([]byte(resp), addr)
    }
}

func check(laddr, raddr string) {
    addr, err := net.ResolveUDPAddr("udp", laddr)
    if err != nil {
	log.Printf("ResolveUDPAddr: %v\n", err)
	return
    }
    conn, err := net.ListenUDP("udp", addr)
    if err != nil {
	log.Printf("ListenUDP: %v\n", err)
	return
    }
    addr, err = net.ResolveUDPAddr("udp", raddr)
    if err != nil {
	log.Printf("ResolveUDPAddr: %v\n", err)
	return
    }
    conn.WriteToUDP([]byte("Probe"), addr)
    buf := make([]byte, 1500)
    n, _, _ := conn.ReadFromUDP(buf)
    fmt.Printf("Remote %s\n", string(buf[:n]))
}

func start_dummy_server(s *Stream) {
    s.Logf("start dummy\n")
    // dummy reader
    go func() {
	buf := make([]byte, RDWRSZ)
	cnt := 0
	for s.running {
	    n, _ := s.Read(buf)
	    s.Tracef("recv %d bytes\n", n)
	    // check count
	    for i := 0; i < n; i++ {
		if buf[i] != byte(cnt) {
		    s.Tracef("mismatch buf[%d]=%d != %d\n", i, buf[i], cnt)
		    cnt = int(buf[i])
		}
		cnt = (cnt + 1) % 256
	    }
	}
    }()
    // dummy writer
    go func() {
	for s.running {
	    time.Sleep(time.Minute)
	    msg := "message from server"
	    s.Write([]byte(msg))
	}
    }()
}

func server(laddr, raddr string) {
    u, err := NewUDPConn(laddr, raddr)
    if err != nil {
	log.Printf("NewUDPConn: %v\n", err)
	return
    }
    u.Connect()
    u.handler = func(s *Stream, remote string) {
	log.Printf("[sid:%d key:%d]New stream for %s\n", s.sid, s.key, remote)
	if remote == "dummy" {
	    start_dummy_server(s)
	    return
	}
	// try to connect local
	conn, err := session.Dial(remote)
	if err != nil {
	    s.Logf("Dial error %v\n", err)
	    return
	}
	// conn will be closed in writer side
	// reader side
	go func() {
	    buf := make([]byte, RDWRSZ)
	    for s.running {
		n, _ := s.Read(buf)
		out := buf[:n]
		o := 0
		for n > 0 {
		    w, err := conn.Write(out[o:])
		    if err != nil {
			s.Logf("remote write error %v\n", err)
			break
		    }
		    o += w
		    n -= w
		}
		if n > 0 {
		    break
		}
	    }
	    s.Logf("try to stop stream (reader side)\n")
	    s.running = false
	}()
	// writer side
	go func() {
	    buf := make([]byte, RDWRSZ)
	    for s.running {
		n, err := conn.Read(buf)
		if err != nil {
		    s.Logf("remote read error %v\n", err)
		    break
		}
		if n == 0 {
		    s.Logf("remote conn closed\n")
		    break
		}
		// push it to stream
		s.Write(buf[:n])
	    }
	    s.Logf("try to stop stream (writer side)\n")
	    s.running = false
	    // wait a bit before closing conn
	    time.Sleep(time.Second)
	    conn.Close()
	}()
    }
    for u.running {
	time.Sleep(time.Second)
    }
}

func dummy_stream(u *UDPconn) {
    for u.running {
	s := u.OpenStream("dummy")
	if s == nil {
	    time.Sleep(100 * time.Millisecond)
	    continue
	}
	// reader in client
	go func() {
	    buf := make([]byte, 1024)
	    for s.running {
		n, _ := s.Read(buf)
		s.Tracef("recv %d bytes %s\n", n, string(buf[:32]))
	    }
	}()
	cnt := 0
	for {
	    time.Sleep(5 * time.Second)
	    buf := make([]byte, 2000)
	    for i := 0; i < 2000; i++ {
		buf[i] = byte(cnt)
		cnt++
	    }
	    s.Write(buf)
	}
    }
}

func client(laddr, raddr, listen, remote string) {
    u, err := NewUDPConn(laddr, raddr)
    if err != nil {
	log.Printf("NewUDPConn: %v\n", err)
	return
    }
    u.Connect()
    time.Sleep(100 * time.Millisecond)
    //
    //go dummy_stream(u)
    // start listening
    serv, err := session.NewServer(listen, func(conn net.Conn) {
	log.Printf("accepted\n")
	defer conn.Close()
	s := u.OpenStream(remote)
	for i := 0; i < 10; i++ {
	    for s == nil {
		if !u.running {
		    break
		}
		time.Sleep(100 * time.Millisecond)
		s = u.OpenStream(remote)
	    }
	}
	if s == nil {
	    log.Printf("unable to open stream\n")
	    return
	}
	// reader in this stream
	go func() {
	    buf := make([]byte, RDWRSZ)
	    for s.running {
		n, _ := s.Read(buf)
		out := buf[:n]
		o := 0
		for n > 0 {
		    w, err := conn.Write(out[o:])
		    if err != nil {
			s.Logf("remote write error %v\n", err)
			break
		    }
		    o += w
		    n -= w
		}
		if n > 0 {
		    break
		}
	    }
	    // stop stream
	    s.Logf("try to stop stream (reader side)\n")
	    s.running = false
	}()
	for s.running {
	    buf := make([]byte, RDWRSZ)
	    n, err := conn.Read(buf)
	    if err != nil {
		s.Logf("local read error %v\n", err)
		break
	    }
	    if n == 0 {
		s.Logf("local conn closed\n")
		break
	    }
	    // push it to remote
	    s.Write(buf[:n])
	}
	// stop stream
	s.Logf("try to stop stream (writer side)\n")
	s.running = false
	// wait a bit before closing conn
	time.Sleep(time.Second)
    })
    log.Printf("start listening on %s\n", listen)
    serv.Run()
}

func main() {
    f, _ := os.Create("uuconn.log")
    log.SetFormatter(&log.JSONFormatter{TimestampFormat:"2006-01-02 15:04:05.000000"})
    mw := io.MultiWriter(os.Stderr, f)
    log.SetOutput(mw)
    log.SetLevel(log.DebugLevel)
    log.Println("start")
    rand.Seed(time.Now().Unix())
    // uuconn command options
    if len(os.Args) < 3 {
	log.Println("uuconn command options...")
	return
    }
    cmd := os.Args[1]
    args := os.Args[2:]
    switch cmd {
    case "checker":
	checker(args[0])
	return
    case "check":
	if len(args) < 2 {
	    log.Println("uuconn check laddr checker")
	    return
	}
	check(args[0], args[1])
	return
    case "server":
	if len(args) < 2 {
	    log.Println("uuconn server laddr raddr")
	    return
	}
	server(args[0], args[1])
	return
    case "client":
	if len(args) < 3 {
	    log.Println("uuconn client laddr raddr listen")
	    return
	}
	client(args[0], args[1], args[2], args[3])
	return
    }
    log.Println("end")
}
