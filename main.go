// MIT License Copyright(c) 2020 Hiroshi Shimamoto
// vim: set sw=4 sts=4:
package main
import (
    "fmt"
    "log"
    "net"
    "os"
    "time"

    "github.com/hshimamoto/go-session"
)

type UDPconn struct {
    addr *net.UDPAddr
    conn *net.UDPConn
    running bool
    connected bool
    queue chan []byte
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
    }
}

func (u *UDPconn)Sender() {
    ticker := time.NewTicker(time.Second)
    for u.running {
	select {
	case <-ticker.C:
	    u.conn.WriteToUDP([]byte("Probe"), u.addr)
	}
    }
}

func (u *UDPconn)Connect() {
    // start receiver
    u.running = true
    u.connected = false
    go u.Receiver()
    // connect
    for u.connected == false {
	u.conn.WriteToUDP([]byte("Probe"), u.addr)
	time.Sleep(100 * time.Millisecond)
    }
    // start sender
    go u.Sender()
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
    go func() {
	buf := make([]byte, 1500)
	for {
	    n, _, _ := conn.ReadFromUDP(buf)
	    log.Printf("recv %s\n", string(buf[:n]))
	}
    }()
    addr, err = net.ResolveUDPAddr("udp", raddr)
    for {
	conn.WriteToUDP([]byte("Probe"), addr)
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
