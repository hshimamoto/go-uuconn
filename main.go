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
    in, out *StreamBuffer
}

func NewStream(sz int) *Stream {
    s := &Stream{}
    s.in = NewStreamBuffer(sz)
    s.out = NewStreamBuffer(sz)
    return s
}

type Message struct {
    mtype int
    seq0, seq1 int
    data []byte
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
    //
    u.stream = NewStream(65536)
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
	log.Printf("%d bytes from %v\n", n, addr)
	// parse
	msg := &Message{}
	msg.mtype = int(buf[0])
	msg.seq0 = int(binary.LittleEndian.Uint16(buf[1:]))
	msg.seq1 = int(binary.LittleEndian.Uint16(buf[3:]))
	msg.data = buf[5:]
	if msg.mtype == 0x41 || msg.mtype == 0x44 {
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
    // uplink buffer
    ulbuf := []byte("TEST MESSAGE TEST MESSAGE TEST MESSAGE TEST MESSAGE")
    ulptr := 0
    ulack := 0
    ulseq := 0
    ackseq := 0
    ackflag := false
    buflen := len(ulbuf)
    dlseq := 0
    q := make(chan bool, 32)
    ackq := make(chan bool, 32)
    ticker := time.NewTicker(time.Second)
    //
    for u.running {
	offset := 0
	for ulptr < buflen {
	    datalen := buflen
	    if datalen > 10 {
		datalen = 10
	    }
	    msglen := 1 + 2 + 2 + datalen
	    seq := ulseq + offset
	    buf := make([]byte, msglen)
	    buf[0] = 0x44 // Data
	    binary.LittleEndian.PutUint16(buf[1:], uint16(seq))
	    binary.LittleEndian.PutUint16(buf[3:], uint16(seq + datalen))
	    copy(buf[5:], ulbuf)
	    u.queue <- buf
	    ulptr += datalen
	    offset += datalen
	    ticker.Reset(100 * time.Millisecond)
	}
	select {
	case msg := <-u.mq:
	    log.Printf("dequeue message type: %d\n", msg.mtype)
	    if msg.mtype == 0x44 {
		if msg.seq0 == dlseq {
		    log.Printf("Data seq %d-%d\n", msg.seq0, msg.seq1)
		    dlseq = msg.seq1
		    ackseq = msg.seq1
		    if ackflag == false {
			ackq <-true
			ackflag = true
		    }
		}
	    }
	    if msg.mtype == 0x41 {
		log.Printf("ack %d\n", msg.seq0)
		ulack = msg.seq0
		ulseq = ulack
	    }
	case <-ticker.C:
	    // rewind
	    if ulptr != ulack {
		log.Printf("rewind %d->%d\n", ulptr, ulack)
		ulptr = ulack
	    }
	    ticker.Reset(time.Second)
	case <-ackq:
	    // ack!
	    buf := make([]byte, 5)
	    buf[0] = 0x41
	    binary.LittleEndian.PutUint16(buf[1:], uint16(ackseq))
	    binary.LittleEndian.PutUint16(buf[3:], uint16(ackseq)) // dummy
	    u.queue <- buf
	    ackflag = false
	case <-q:
	    // ignore
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
	time.Sleep(time.Second)
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
