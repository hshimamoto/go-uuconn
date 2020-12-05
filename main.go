// MIT License Copyright(c) 2020 Hiroshi Shimamoto
// vim: set sw=4 sts=4:
package main
import (
    "log"
    "net"
    "os"

    "github.com/hshimamoto/go-session"
)

func main() {
    log.Println("start")
    laddr := "127.0.0.1:19998"
    if len(os.Args) > 1 {
	laddr = os.Args[1]
    }
    log.Printf("local addr %s\n", laddr)
    serv, err := session.NewServer(laddr, func(conn net.Conn) {
    })
    if err != nil {
	log.Println(err)
	return
    }
    serv.Run()
    log.Println("end")
}
