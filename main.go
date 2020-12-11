// MIT License Copyright(c) 2020 Hiroshi Shimamoto
// vim: set sw=4 sts=4:
package main
import (
    "encoding/binary"
    "fmt"
    "log"
    "net"
    "os"
    "time"

    "github.com/hshimamoto/go-session"
)

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
    in, out *StreamBuffer
    running bool
    mq chan *Message
    tq chan bool // timer queue
    sendq chan []byte
}

func NewStream(sid, sz int) *Stream {
    s := &Stream{}
    s.sid = sid
    s.in = NewStreamBuffer(sz)
    s.out = NewStreamBuffer(sz)
    s.running = false
    s.mq = make(chan *Message, 32)
    s.tq = make(chan bool, 32)
    s.sendq = make(chan []byte, 32)
    return s
}

func (s *Stream)Runner(queue chan<- []byte) {
    // uplink buffer
    ulbuf := []byte("TEST MESSAGE TEST MESSAGE TEST MESSAGE TEST MESSAGE")
    ulptr := 0
    ulack := 0
    ulseq := 0
    ackseq := 0
    ackflag := false
    buflen := len(ulbuf)
    lastseq := buflen
    dlseq := 0
    ultime := time.Now()
    q := make(chan bool, 32)
    ackq := make(chan bool, 32)
    ticker := time.NewTicker(time.Second)
    mss := 200
    for s.running {
	if ulack == lastseq {
	    select {
	    case next := <-s.sendq:
		ulbuf = next
		buflen = len(ulbuf)
		ulptr = ulseq
		lastseq = (ulseq + buflen) % 65536
		log.Printf("dequeue send message %d bytes lastseq=%d\n", buflen, lastseq)
	    default:
	    }
	}
	offset := 0
	if ulptr != lastseq {
	    ultime = time.Now().Add(500 * time.Millisecond)
	}
	for ulptr != lastseq {
	    datalen := ((lastseq + 65536) - ulptr) % 65536
	    if datalen > mss {
		datalen = mss
	    }
	    seq0 := (ulseq + offset) % 65536
	    seq1 := (ulseq + offset + datalen) % 65536
	    msg := &Message{
		mtype: MSG_DATA,
		sid: 0,
		seq0: seq0,
		seq1: seq1,
	    }
	    buf := msg.Pack()
	    queue <- buf
	    ulptr = seq1
	    offset += datalen
	}
	select {
	case msg := <-s.mq:
	    switch msg.mtype {
	    case MSG_DATA:
		log.Printf("Data seq %d-%d\n", msg.seq0, msg.seq1)
		if msg.seq0 == dlseq {
		    if ackseq != msg.seq1 {
			log.Printf("Change ackseq %d -> %d\n", ackseq, msg.seq1)
		    }
		    dlseq = msg.seq1
		    ackseq = msg.seq1
		}
		if ackflag == false {
		    log.Printf("Queue Ack\n")
		    ackq <-true
		    ackflag = true
		}
	    case MSG_ACK:
		log.Printf("Ack seq %d-%d\n", msg.seq0, msg.seq1)
		ulack = msg.seq0
		ulseq = ulack
	    }
	case <-s.tq:
	    // must wait a bit
	    if time.Now().After(ultime) {
		if ulseq != lastseq {
		    log.Printf("rewind %d->%d (%d)\n", ulptr, ulseq, lastseq)
		    ulptr = ulseq
		}
	    }
	case <-ticker.C:
	    s.tq <- true
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
	    log.Printf("Send Ack %d\n", ackseq)
	    // ack!
	    msg := &Message {
		mtype: MSG_ACK,
		sid: 0,
		seq0: ackseq,
		seq1: ackseq,
	    }
	    buf := msg.Pack()
	    queue <- buf
	    ackflag = false
	case <-q:
	    // ignore
	}
    }
}

type Message struct {
    mtype int
    sid int
    seq0, seq1 int
    data []byte
}

const MSG_DATA	int = 0x44 // Data
const MSG_ACK	int = 0x41 // Ack
const MSG_PROBE	int = 0x50 // Probe

func (m *Message)Pack() []byte {
    datalen := len(m.data)
    msglen := 1 + 1 + 2 + 2 + datalen
    buf := make([]byte, msglen)
    buf[0] = byte(m.mtype)
    buf[1] = byte(m.sid)
    binary.LittleEndian.PutUint16(buf[2:], uint16(m.seq0))
    binary.LittleEndian.PutUint16(buf[4:], uint16(m.seq1))
    copy(buf[6:], m.data)
    return buf
}

func ParseMessage(buf []byte) *Message {
    msg := &Message{}
    msg.mtype = int(buf[0])
    msg.sid = int(buf[1])
    msg.seq0 = int(binary.LittleEndian.Uint16(buf[2:]))
    msg.seq1 = int(binary.LittleEndian.Uint16(buf[4:]))
    msg.data = buf[6:]
    return msg
}

type UDPconn struct {
    addr *net.UDPAddr
    conn *net.UDPConn
    running bool
    connected bool
    queue chan []byte
    mq chan *Message
    stream *Stream
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
    return u, err
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
	    continue
	}
	// parse
	msg := ParseMessage(buf[:n])
	if msg.mtype == MSG_DATA || msg.mtype == MSG_ACK {
	    u.mq <- msg
	}
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
    //
    u.stream = NewStream(0, 65536)
    // start stream
    u.stream.running = true
    go u.stream.Runner(u.queue)
    for u.running {
	select {
	case msg := <-u.mq:
	    u.stream.mq <- msg
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
    log.Printf("Remote %s\n", string(buf[:n]))
}

func server(laddr, raddr, caddr string) {
    u, err := NewUDPConn(laddr, raddr)
    if err != nil {
	log.Printf("NewUDPConn: %v\n", err)
	return
    }
    u.Connect()
    for u.running {
	time.Sleep(time.Second)
    }
}

func client(laddr, raddr, listen string) {
    u, err := NewUDPConn(laddr, raddr)
    if err != nil {
	log.Printf("NewUDPConn: %v\n", err)
	return
    }
    u.Connect()
    for u.running {
	time.Sleep(5 * time.Second)
	msg := fmt.Sprintf("feed new message at %v\n", time.Now())
	for dummy := 0; dummy < 100; dummy++ {
	    msg += "DUMMYDUMMY"
	    msg += "dummydummy"
	    msg += "DUMMYDUMMY"
	    msg += "dummydummy"
	}
	u.stream.sendq <- []byte(msg)
    }
}

func main() {
    log.Println("start")
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
	if len(args) < 3 {
	    log.Println("uuconn server laddr raddr caddr")
	    return
	}
	server(args[0], args[1], args[2])
	return
    case "client":
	if len(args) < 3 {
	    log.Println("uuconn client laddr raddr listen")
	    return
	}
	client(args[0], args[1], args[2])
	return
    }
    laddr := "127.0.0.1:13389"
    serv, err := session.NewServer(laddr, func(conn net.Conn) {
    })
    if err != nil {
	log.Println(err)
	return
    }
    log.Println("start serv")
    serv.Run()
    log.Println("end")
}
