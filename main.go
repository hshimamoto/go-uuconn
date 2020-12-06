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
    return u, err
}

func (u *UDPconn)Connect(remote string) {
    conn, err := net.ListenUDP("udp", u.addr)
    if err != nil {
	log.Printf("failed to listen %s\n", u.addr)
	return
    }
    // start receiver
    found := false
    go func() {
	buf := make([]byte, 1500)
	for {
	    n, addr, err := conn.ReadFromUDP(buf)
	    if err != nil {
		log.Printf("Read: %v\n", err)
		continue
	    }
	    log.Printf("%d bytes from %v\n", n, addr)
	    found = true
	}
    }()
    u.conn = conn
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
