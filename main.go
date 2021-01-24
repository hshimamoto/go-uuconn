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
    "sync"
    "strings"
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

type Blob struct {
    data []byte
    // seq
    first, last int
    ptr, ack int
    inflight int
    blkid int
    ready bool
    //
    msgs []*Message
    sacks []int
}

func (b *Blob)MessageSetup(s *Stream, pb *Blob, blkid int) {
    if b.ready {
	return
    }
    prev := pb.last
    blen := len(b.data)
    b.first = prev
    b.last = (prev + blen) % 65536
    b.ptr = prev
    b.ack = prev
    b.blkid = blkid
    b.inflight = 0
    b.msgs = []*Message{}
    offset := 0
    ptr := b.first
    for ptr != b.last {
	datalen := ((b.last + 65536) - ptr) % 65536
	if datalen > MSS {
	    datalen = MSS
	}
	seq0 := (b.first + offset) % 65536
	seq1 := (b.first + offset + datalen) % 65536
	msg := &Message{
	    mtype: MSG_DATA,
	    sid: s.sid,
	    key: s.key,
	    blkid: b.blkid,
	    seq0: seq0,
	    seq1: seq1,
	}
	msg.data = make([]byte, datalen)
	copy(msg.data, b.data[offset:offset+datalen])
	//msg.data = b.data[offset:offset+datalen]
	b.msgs = append(b.msgs, msg)
	offset += datalen
	ptr = msg.seq1
    }
    s.Tracef("setup %d msgs\n", len(b.msgs))
    b.ready = true
}

func (b *Blob)Transfer(s *Stream, queue chan<- []byte) {
    for _, msg := range b.msgs {
	s.Tracef("Push Data seq %d-%d [%d]\n", msg.seq0, msg.seq1, msg.data[0])
	queue <- msg.Pack()
	b.inflight++
	b.ptr = msg.seq1
    }
}

func (b *Blob)Ack(s *Stream, ack int, sacks []int) {
    b.ack = ack
    b.sacks = sacks
}

func (b *Blob)Rebuild(s *Stream) {
    // don't rebuild small number of msgs
    if len(b.msgs) < 3 {
	return
    }
    msgs := b.msgs
    b.msgs = []*Message{}
    skip := true
    for _, msg := range msgs {
	if skip {
	    s.Tracef("drop %d-%d\n", msg.seq0, msg.seq1)
	    if msg.seq1 == b.ack {
		skip = false
	    }
	    continue
	}
	hit := false
	for _, sack := range b.sacks {
	    if msg.seq0 == sack {
		s.Tracef("sack drop %d-%d\n", msg.seq0, msg.seq1)
		hit = true
		break
	    }
	}
	if hit {
	    continue
	}
	s.Tracef("requeue %d-%d\n", msg.seq0, msg.seq1)
	b.msgs = append(b.msgs, msg)
    }
    if len(b.msgs) == 0 {
	s.Tracef("unable to drop\n")
	b.msgs = msgs
    }
}

func (b *Blob)Rewind(s *Stream, t string) {
    s.Tracef("%s rewind %d to %d ack %d (%d) inflight %d\n", t, b.ptr, b.first, b.ack, b.last, b.inflight)
    b.Rebuild(s)
    b.ptr = b.ack
    b.inflight = 0
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
    recvq_enq, recvq_deq int
    established bool
    recv []byte
    rmtx sync.Mutex
    wmtx sync.Mutex
    now_reading bool
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
    s.now_reading = false
}

func (s *Stream)Tracef(fmt string, a ...interface{}) {
    args := make([]interface{}, len(a) + 1)
    args[0] = s.sid
    copy(args[1:], a)
    log.Tracef("[%d] " + fmt, args...)
}

func (s *Stream)Debugf(fmt string, a ...interface{}) {
    args := make([]interface{}, len(a) + 1)
    args[0] = s.sid
    copy(args[1:], a)
    log.Debugf("[%d] " + fmt, args...)
}

func (s *Stream)Logf(fmt string, a ...interface{}) {
    args := make([]interface{}, len(a) + 1)
    args[0] = s.sid
    copy(args[1:], a)
    log.Infof("[%d] " + fmt, args...)
}

func (s *Stream)Runner(queue chan<- []byte) {
    // uplink buffer blob
    var b, pending *Blob
    b = &Blob{}
    b.first = 0
    b.last = 0
    b.ptr = 0
    b.ack = 0
    b.inflight = 0
    pending = nil
    // local variables
    blkid := 0
    ackseq := 0
    ackblkid := 0
    ackflag := false
    dlseq := 0
    dupack := 0
    fastrewindack := 0
    resend := time.Duration(100)
    ultime := time.Now()
    ackq := make(chan bool, 32)
    ticker := time.NewTicker(time.Second)
    lastrecv := time.Now().Add(time.Minute)
    keepalive := time.Now().Add(10 * time.Second)
    nr_replace := 0
    nr_append := 0
    nr_rewind := 0
    nr_badack := 0
    pool := []*Message{}
    msgsz := 32768 + 16384
    for s.running {
	if pending == nil || pending.ready == false {
	    select {
	    case next := <-s.sendq:
		blen := 0
		if b != nil {
		    blen = len(b.data)
		}
		pendinglen := 0
		if pending != nil {
		    pendinglen = len(pending.data)
		}
		s.Tracef("dequeue %d bytes (current %d, pending %d)\n", len(next), blen, pendinglen)
		if pending != nil {
		    pending.data = append(pending.data, next...)
		    nr_append++
		} else {
		    pending = &Blob{ data: next }
		    blkid++
		}
		if len(pending.data) > msgsz {
		    pending.MessageSetup(s, b, blkid)
		    // and now pending.ready is true
		}
	    default:
	    }
	}
	if pending != nil {
	    if b.ack == b.last {
		pending.MessageSetup(s, b, blkid)
		// replace
		b = pending
		pending = nil
		// rest
		resend = 100
		s.Tracef("replace blob last=%d (prev %d)\n", b.last, b.first)
		nr_replace++
		// send NEXT for drop pool
		msg := &Message{
		    mtype: MSG_NEXT,
		    sid: s.sid,
		    key: s.key,
		    seq0: 0,
		    seq1: 0,
		}
		queue <- msg.Pack()
	    }
	}
	if b.ptr != b.last {
	    ultime = time.Now().Add(time.Millisecond * resend)
	    ticker.Reset(resend * time.Millisecond)
	    //
	    b.Transfer(s, queue)
	}
	select {
	case msg := <-s.mq:
	    lastrecv = time.Now().Add(time.Minute)
	    if s.established == false {
		s.Logf("Established\n")
	    }
	    s.established = true
	    switch msg.mtype {
	    case MSG_DATA:
		s.Tracef("MSG: Data seq %d-%d [%d]\n", msg.seq0, msg.seq1, msg.data[0])
		if msg.seq0 == dlseq {
		    if ackseq != msg.seq1 {
			s.Tracef("Change ackseq %d to %d\n", ackseq, msg.seq1)
		    }
		    s.recvq <- msg.data
		    s.Tracef("recvq: enqueue %d bytes %d\n", len(msg.data), s.recvq_enq)
		    s.recvq_enq += len(msg.data)
		    dlseq = msg.seq1
		    ackseq = msg.seq1
		    ackblkid = msg.blkid
		    // check pool
		    retry := len(pool) > 0
		    for retry {
			retry = false
			pool2 := []*Message{}
			s.Tracef("check pool %d\n", len(pool))
			for _, m := range pool {
			    if m.blkid != ackblkid {
				continue
			    }
			    if m.seq0 == dlseq {
				if ackseq != m.seq1 {
				    s.Tracef("Change ackseq %d to %d [pool]\n", ackseq, m.seq1)
				}
				s.recvq <- m.data
				s.Tracef("recvq: enqueue %d bytes %d\n", len(m.data), s.recvq_enq)
				s.recvq_enq += len(m.data)
				dlseq = m.seq1
				ackseq = m.seq1
				retry = true
				continue
			    }
			    pool2 = append(pool2, m)
			}
			pool = pool2
		    }
		} else {
		    // pool it
		    hit := false
		    for _, m := range pool {
			if m.seq0 == msg.seq0 {
			    hit = true
			    break
			}
		    }
		    if !hit {
			pool = append(pool, msg)
			s.Tracef("pool %d-%d\n", msg.seq0, msg.seq1)
		    }
		}
		if ackflag == false {
		    ackq <-true
		    ackflag = true
		}
	    case MSG_ACK:
		s.Tracef("MSG: Ack seq %d-%d %d\n", msg.seq0, msg.seq1, len(msg.data))
		// show sack
		sack := []int{}
		for n := 0; n < len(msg.data); n += 2 {
		    seq := int(binary.LittleEndian.Uint16(msg.data[n:]))
		    diff := (65536 + seq - b.first) % 65536
		    if diff < msgsz + 4096 {
			s.Tracef("sack %d\n", seq)
			sack = append(sack, seq)
		    } else {
			s.Tracef("sack %d invalid\n", seq)
		    }
		}
		diff := (65536 + msg.seq0 - b.ack) % 65536
		if diff < msgsz + 4096 {
		    if b.ack == msg.seq0 {
			dupack++
		    } else {
			dupack = 0
		    }
		    if dupack >= 5 {
			if fastrewindack != b.ack {
			    b.Rewind(s, "fast")
			    nr_rewind++
			    dupack = 0
			    fastrewindack = b.ack
			}
		    }
		    b.Ack(s, msg.seq0, sack)
		} else {
		    nr_badack++
		}
	    case MSG_NEXT:
		s.Tracef("MSG: Next drop pool %d\n", len(pool))
		pool = []*Message{}
	    }
	    // ignore KEEP
	case <-ticker.C:
	    // must wait a bit
	    if time.Now().After(ultime) {
		if b.ack != b.last {
		    b.Rewind(s, "slow")
		    nr_rewind++
		    resend += 100
		}
	    }
	    // keep alive
	    if time.Now().After(keepalive) {
		keep := &Message{
		    mtype: MSG_KEEP,
		    sid: s.sid,
		    key: s.key,
		    seq0: 0,
		    seq1: 0,
		}
		queue <- keep.Pack()
		keepalive = time.Now().Add(10 * time.Second)
	    }
	    if time.Now().After(lastrecv) {
		s.Logf("no activity\n")
		// close stream
		s.running = false
	    }
	    if b.ack == b.last {
		ticker.Reset(time.Second)
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
		blkid: ackblkid,
		seq0: ackseq,
		seq1: ackseq,
	    }
	    // sack
	    sacklen := len(pool) * 2
	    msg.data = make([]byte, sacklen)
	    for i, m := range pool {
		binary.LittleEndian.PutUint16(msg.data[i*2:], uint16(m.seq0))
	    }
	    buf := msg.Pack()
	    queue <- buf
	    ackflag = false
	case <-s.bell:
	    // ignore
	}
    }
    ticker.Stop()
    //
    if s.now_reading {
	s.recvq <- []byte("")
	s.Logf("push empty buf to stop Read\n")
    }
    // clear queue
    s.key = -1
    time.Sleep(time.Second)
    empty := false
    cnt := 0
    for !empty {
	select {
	case <-s.mq:
	    cnt++
	case <-s.sendq:
	    cnt++
	case <-s.bell:
	    cnt++
	default:
	    empty = true
	}
    }
    // close channels
    close(s.mq)
    s.mq = nil
    close(s.sendq)
    s.sendq = nil
    close(s.bell)
    s.bell = nil
    close(ackq)
    ackq = nil
    s.Debugf("discard %d items\n", cnt)
    //
    s.Debugf("stats %d append %d replace %d rewind %d badack\n", nr_append, nr_replace, nr_rewind, nr_badack)
    time.Sleep(time.Minute)
    // make it's free
    s.used = false
    log.Printf("stream %d is now free\n", s.sid)
}

func (s *Stream)StartRunner(queue chan<- []byte) {
    log.Printf("start Runner %d %d\n", s.sid, s.key)
    s.running = true
    go s.Runner(queue)
}

func (s *Stream)Read(buf []byte) (int, error) {
    s.rmtx.Lock()
    s.now_reading = true
    defer func() {
	s.now_reading = false
	s.rmtx.Unlock()
    }()
    for s.recv == nil {
	next := []byte(nil)
	select {
	case next = <-s.recvq:
	default:
	    if s.running == false {
		// EOF
		return 0, io.EOF
	    }
	    // wait recv
	    next = <-s.recvq
	}
	s.recv = next
	if s.recv != nil {
	    s.Tracef("recvq: dequeue %d bytes %d\n", len(s.recv), s.recvq_deq)
	    s.recvq_deq += len(s.recv)
	}
    }
    if s.recv != nil && len(s.recv) == 0 {
	return 0, io.EOF
    }
    // try to recv more
    if len(s.recv) < len(buf) {
	select {
	case next := <-s.recvq:
	    s.recv = append(s.recv, next...)
	default:
	}
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
    if s.running == false {
	// EOF
	return 0, io.EOF
    }
    sendbuf := make([]byte, len(buf))
    n := len(buf)
    s.wmtx.Lock()
    defer s.wmtx.Unlock()
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
    blkid int
    seq0, seq1 int
    data []byte
}

const MSG_DATA	int = 0x44 // Data
const MSG_ACK	int = 0x41 // Ack
const MSG_KEEP	int = 0x4B // Keep
const MSG_OPEN	int = 0x4f // Open
const MSG_RESET	int = 0x52 // Reset
const MSG_PROBE	int = 0x50 // Probe
const MSG_probe	int = 0x70 // probe
const MSG_NEXT	int = 0x4e // Next

func (m *Message)Pack() []byte {
    datalen := len(m.data)
    msglen := 1 + 1 + 2 + 2 + 2 + 2 + datalen
    buf := make([]byte, msglen)
    buf[0] = byte(m.mtype)
    buf[1] = byte(m.sid)
    binary.LittleEndian.PutUint16(buf[2:], uint16(m.key))
    binary.LittleEndian.PutUint16(buf[4:], uint16(m.blkid))
    binary.LittleEndian.PutUint16(buf[6:], uint16(m.seq0))
    binary.LittleEndian.PutUint16(buf[8:], uint16(m.seq1))
    copy(buf[10:], m.data)
    return buf
}

func ParseMessage(buf []byte) *Message {
    msg := &Message{}
    msg.mtype = int(buf[0])
    msg.sid = int(buf[1])
    if len(buf) < 10 {
	return msg
    }
    msg.key = int(binary.LittleEndian.Uint16(buf[2:]))
    msg.blkid = int(binary.LittleEndian.Uint16(buf[4:]))
    msg.seq0 = int(binary.LittleEndian.Uint16(buf[6:]))
    msg.seq1 = int(binary.LittleEndian.Uint16(buf[8:]))
    msg.data = make([]byte, len(buf[10:]))
    copy(msg.data, buf[10:])
    return msg
}

type UDPremote struct {
    addr *net.UDPAddr
    raddr string
    live bool
    running bool
    connected bool
    queue chan []byte
    mq chan *Message
    streams []*Stream
    nr_streams int
    handler func(s *Stream, remote string)
    mtx sync.Mutex
}

func remote_handler(s *Stream, remote string) {
    log.Printf("[sid:%d key:%d]New stream for %s\n", s.sid, s.key, remote)
    // try to connect local
    conn, err := session.Dial(remote)
    if err != nil {
	s.Logf("Dial error %v\n", err)
	return
    }
    log.Printf("connected to %s\n", remote)
    // conn will be closed in writer side
    // reader side
    reader_alive := true
    go func() {
	buf := make([]byte, RDWRSZ)
	for s.running {
	    n, _ := s.Read(buf)
	    if n == 0 {
		s.Logf("remote stream is null\n")
		break
	    }
	    w, err := conn.Write(buf[:n])
	    if err != nil {
		s.Logf("remote write error %v\n", err)
		break
	    }
	    if w != n {
		s.Logf("remote write only %d of %d\n", w, n)
		break
	    }
	}
	s.Logf("try to stop stream (reader side)\n")
	s.running = false
	reader_alive = false
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
	for reader_alive {
	    s.Logf("wait to stop reader side\n")
	    time.Sleep(time.Second)
	}
	conn.Close()
	s.Logf("finish remote_handler")
    }()
}

func NewUDPremote(raddr string) (*UDPremote, error) {
    addr, err := net.ResolveUDPAddr("udp", raddr)
    if err != nil {
	return nil, err
    }
    r := &UDPremote{}
    r.addr = addr
    r.raddr = addr.String()
    r.live = false
    r.running = false
    r.queue = make(chan []byte, 32)
    r.mq = make(chan *Message, 32)
    r.nr_streams = 64
    r.streams = make([]*Stream, r.nr_streams)
    for i, _ := range r.streams {
	r.streams[i] = NewStream(i, 65536)
    }
    r.handler = remote_handler
    return r, nil
}

func (r *UDPremote)String() string {
    return r.raddr
}

func (r *UDPremote)AllocStream(n int) *Stream {
    r.mtx.Lock()
    defer r.mtx.Unlock()
    if n < 0 {
	for _, s := range r.streams {
	    if s.used {
		continue
	    }
	    // mark it
	    s.used = true
	    return s
	}
    } else {
	if n >= r.nr_streams {
	    return nil
	}
	s := r.streams[n]
	if s.used {
	    return nil
	}
	// mark it
	s.used = true
	return s
    }
    return nil
}

func (r *UDPremote)OpenStream(remote string) *Stream {
    s := r.AllocStream(-1)
    if s == nil {
	log.Printf("OpenStream: no slot\n")
	return s
    }
    s.Init(rand.Intn(65536))
    log.Printf("[sid:%d key:%d] try to open %s\n", s.sid, s.key, remote)
    s.StartRunner(r.queue)
    for i := 0; i < 10; i++ {
	msg := &Message{
	    mtype: MSG_OPEN,
	    sid: s.sid,
	    key: s.key,
	    seq0: len(remote),
	    seq1: len(remote),
	    data: []byte(remote),
	}
	r.queue <- msg.Pack()
	time.Sleep(100 * time.Millisecond)
	if !s.running {
	    log.Printf("[sid:%d key:%d] stop running\n", s.sid, s.key)
	    break
	}
	if s.established {
	    log.Printf("[sid:%d key:%d] established\n", s.sid, s.key)
	    break
	}
    }
    if !s.established {
	log.Printf("[sid:%d key:%d] failed to open\n", s.sid, s.key)
	// stopping
	s.running = false
	return nil
    }
    return s
}

func (r *UDPremote)Sender(queue chan *SendToPair) {
    log.Infof("start remote Sender\n")
    ticker := time.NewTicker(10 * time.Second)
    for r.running {
	select {
	case buf := <-r.queue:
	    pair := &SendToPair{
		addr: r.addr,
		data: buf,
	    }
	    queue <- pair
	case <-ticker.C:
	    pair := &SendToPair{
		addr: r.addr,
		data: []byte("Probe"),
	    }
	    queue <- pair
	}
    }
}

func (r *UDPremote)Receiver() {
    log.Infof("start remote Receiver\n")
    lastrecv := time.Now().Add(time.Minute)
    ticker := time.NewTicker(30 * time.Second)
    for r.running {
	select {
	case msg := <-r.mq:
	    lastrecv = time.Now().Add(time.Minute)
	    if r.connected == false {
		log.Infof("connected with %s\n", r.raddr)
		r.connected = true
	    }
	    // probe?
	    switch msg.mtype {
	    case MSG_PROBE:
		r.queue <- []byte("probe")
		break
	    case MSG_probe:
		break
	    }
	    sid := msg.sid
	    if sid < 0 || sid >= r.nr_streams {
		break
	    }
	    s := r.streams[sid]
	    switch msg.mtype {
	    case MSG_DATA, MSG_ACK, MSG_KEEP, MSG_NEXT:
		if s.running && msg.key == s.key {
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
			r.queue <- msg.Pack()
			break
		    }
		} else {
		    // start new stream
		    s.Init(msg.key)
		    s.used = true
		    s.established = true // server side
		    s.StartRunner(r.queue)
		    // call handler
		    if r.handler != nil {
			remote := string(msg.data)
			r.handler(s, remote)
		    }
		}
		keep := &Message{
		    mtype: MSG_KEEP,
		    sid: sid,
		    key: s.key,
		    seq0: 0,
		    seq1: 0,
		}
		log.Printf("ack for OPEN %d %d by keep\n", msg.sid, msg.key)
		r.queue <- keep.Pack()
		r.queue <- keep.Pack()
		r.queue <- keep.Pack()
	    case MSG_RESET:
		log.Printf("recv RESET %d %d\n", msg.sid, msg.key)
		// close the stream
		s.running = false
	    }
	case <-ticker.C:
	    if time.Now().After(lastrecv) {
		log.Infof("no activity on %s\n", r.addr)
		// close stream
		r.running = false
	    }
	}
    }
    ticker.Stop()
    log.Infof("remote %s is closed\n", r.addr)
}

type SendToPair struct {
    addr *net.UDPAddr
    data []byte
}

type UDPconn struct {
    conn *net.UDPConn
    remotes []*UDPremote
    running bool
    queue chan *SendToPair
    api_resp chan string
}

func NewUDPConn() (*UDPconn, error) {
    u := &UDPconn{}
    conn, err := net.ListenUDP("udp", nil)
    if err != nil {
	return nil, err
    }
    u.conn = conn
    u.remotes = []*UDPremote{}
    u.queue = make(chan *SendToPair, 32)
    u.api_resp = nil
    return u, err
}


func (u *UDPconn)Receiver() {
    log.Infof("start Receiver\n")
    conn := u.conn
    buf := make([]byte, 1500)
    for u.running {
	n, addr, err := conn.ReadFromUDP(buf)
	if err != nil {
	    log.Printf("Read: %v\n", err)
	    continue
	}
	raddr := addr.String()
	var remote *UDPremote = nil
	curr_remotes := u.remotes
	for _, r := range curr_remotes {
	    if r.String() == raddr {
		remote = r
		break
	    }
	}
	if remote == nil {
	    if buf[0] == 0x50 { // 'P'robe
		log.Printf("read from %s %s\n", addr.String(), string(buf[:n]))
		fmt.Printf("Remote %s\n", string(buf[:n]))
		resp := u.api_resp
		if resp != nil {
		    resp <- string(buf[:n])
		}
		continue
	    }
	    log.Debugf("unknown remote %s\n", addr.String())
	    continue
	}
	// parse
	msg := ParseMessage(buf[:n])
	remote.mq <- msg
    }
}

func (u *UDPconn)Sender() {
    for u.running {
	select {
	case pair := <-u.queue:
	    u.conn.WriteToUDP(pair.data, pair.addr)
	}
    }
}

func (u *UDPconn)Connect() {
    // start receiver
    u.running = true
    go u.Receiver()
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
    fmt.Printf("Remote %s\n", string(buf[:n]))
}

func server(listen string, reqs []string) {
    u, err := NewUDPConn()
    if err != nil {
	log.Printf("NewUDPConn: %v\n", err)
	return
    }
    u.Connect()
    // start listening
    serv, err := session.NewServer(listen, func(conn net.Conn) {
	api_handler(u, conn)
    })
    if err != nil {
	log.Infof("failed to start listening on %s\n", listen)
	return
    }
    // put request on start up
    go func() {
	time.Sleep(200 * time.Millisecond)
	for _, r := range reqs {
	    do_api(nil, u, r)
	}
    }()
    log.Printf("start listening on %s\n", listen)
    serv.Run()
}

type LocalServer struct {
    u *UDPconn
    serv *session.Server
}

func NewLocalServer(u *UDPconn, listen, remote string) (*LocalServer, error) {
    if len(u.remotes) == 0 {
	return nil, fmt.Errorf("no remotes")
    }
    ls := &LocalServer{ u: u }
    r := u.remotes[0]
    serv, err := session.NewServer(listen, func(conn net.Conn) {
	log.Infof("accepted\n")
	defer conn.Close()
	s := r.OpenStream(remote)
	for i := 0; i < 10; i++ {
	    if s != nil {
		break
	    } else {
		if !u.running {
		    return
		}
	    }
	    time.Sleep(100 * time.Millisecond)
	    s = r.OpenStream(remote)
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
		if n == 0 {
		    s.Logf("local stream is null\n")
		    break
		}
		w, err := conn.Write(buf[:n])
		if err != nil {
		    s.Logf("local write error %v\n", err)
		    break
		}
		if w != n {
		    s.Logf("local write only %d of %d\n", w, n)
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
    if err != nil {
	return nil, err
    }
    ls.serv = serv
    return ls, nil
}

func (ls *LocalServer)Run() {
    ls.serv.Run()
}

func do_api(conn net.Conn, u *UDPconn, request string) {
    request = strings.TrimSpace(request)
    reqs := strings.Split(request, " ")
    cmd := strings.TrimSpace(reqs[0])
    log.Infof("do_api: %s\n", request)
    switch cmd {
    case "CHECK":
	if len(reqs) != 2 {
	    log.Infof("Bad request: %s\n", request)
	    return
	}
	raddr := strings.TrimSpace(reqs[1])
	addr, err := net.ResolveUDPAddr("udp", raddr)
	if err != nil {
	    log.Printf("ResolveUDPAddr: %v\n", err)
	    return
	}
	u.queue <- &SendToPair{
	    addr: addr,
	    data: []byte("Probe"),
	}
	if conn != nil {
	    resp := make(chan string, 32)
	    u.api_resp = resp
	    select {
	    case s := <-resp:
		conn.Write([]byte(s))
	    case <-time.After(time.Second):
	    }
	    u.api_resp = nil
	}
    case "CONNECT":
	raddr := strings.TrimSpace(reqs[1])
	remote, err := NewUDPremote(raddr)
	if err != nil {
	    return
	}
	// to remove dead remote
	curr_remotes := u.remotes
	new_remotes := []*UDPremote{}
	for _, r := range curr_remotes {
	    if r.running {
		new_remotes = append(new_remotes, r)
	    } else {
		log.Infof("%s is removed\n", r.addr)
	    }
	}
	u.remotes = new_remotes
	// check in remote
	curr_remotes = u.remotes
	raddr = remote.String()
	for _, r := range curr_remotes {
	    if r.String() == raddr {
		// already have
		log.Infof("already have connection with %s\n", raddr)
		return
	    }
	}
	u.remotes = append(u.remotes, remote)
	remote.running = true
	go remote.Receiver()
	go remote.Sender(u.queue)
	go func() {
	    cnt := 0
	    for remote.connected == false {
		log.Debugf("send probe to %s\n", remote.String())
		u.queue <- &SendToPair{
		    addr: remote.addr,
		    data: []byte("Probe"),
		}
		time.Sleep(500 * time.Millisecond)
		cnt++
		if cnt % 10 == 0 {
		    time.Sleep(10 * time.Second)
		}
	    }
	}()
    case "ADD":
	if len(reqs) != 3 {
	    log.Infof("Bad request: %s\n", request)
	    return
	}
	listen := strings.TrimSpace(reqs[1])
	remote := strings.TrimSpace(reqs[2])
	serv, err := NewLocalServer(u, listen, remote)
	if err != nil {
	    log.Infof("failed to start listening on %s\n", listen)
	    return
	}
	log.Infof("start listening on %s\n", listen)
	go serv.Run()
    default:
	log.Infof("Unknown API Command: %s\n", cmd)
    }
}

func api_handler(u *UDPconn, conn net.Conn) {
    log.Infof("API handler\n")
    defer conn.Close()
    buf := make([]byte, 1024)
    n, err := conn.Read(buf)
    if err != nil {
	return
    }
    // use only 1st line
    request := strings.Split(string(buf[:n]), "\n")[0]
    do_api(conn, u, request)
}

func client(listen string, reqs []string) {
    log.Debugf("start client\n")
    u, err := NewUDPConn()
    if err != nil {
	log.Printf("NewUDPConn: %v\n", err)
	return
    }
    u.Connect()
    // start listening
    serv, err := session.NewServer(listen, func(conn net.Conn) {
	api_handler(u, conn)
    })
    if err != nil {
	log.Infof("failed to start listening on %s\n", listen)
	return
    }
    // put request on start up
    go func() {
	time.Sleep(200 * time.Millisecond)
	for _, r := range reqs {
	    do_api(nil, u, r)
	}
    }()
    log.Printf("start listening on %s\n", listen)
    serv.Run()
}

func usage() {
    fmt.Println("uuconn command options...")
    os.Exit(1)
}

func main() {
    f, _ := os.Create("uuconn.log")
    args := os.Args
    if len(args) == 1 {
	usage()
    }

    log.SetFormatter(&log.JSONFormatter{TimestampFormat:"2006-01-02 15:04:05.000000"})
    if args[1] == "-v" {
	args = args[1:]
	mw := io.MultiWriter(os.Stderr, f)
	log.SetOutput(mw)
	log.SetLevel(log.DebugLevel)
    } else if args[1] == "-d" {
	args = args[1:]
	mw := io.MultiWriter(os.Stderr, f)
	log.SetOutput(mw)
	log.SetLevel(log.TraceLevel)
    } else {
	log.SetOutput(f)
	log.SetLevel(log.InfoLevel)
    }

    if len(args) < 2 {
	usage()
    }

    log.Println("start")
    defer log.Println("end")
    rand.Seed(time.Now().Unix())
    // uuconn command options
    cmd := args[1]
    args = args[2:]
    switch cmd {
    case "checker":
	checker(args[0])
	return
    case "check":
	if len(args) < 2 {
	    fmt.Println("uuconn check laddr checker")
	    return
	}
	check(args[0], args[1])
	return
    case "server":
	if len(args) < 1 {
	    fmt.Println("uuconn server listen")
	    return
	}
	reqs := []string{}
	if len(args) > 1 {
	    reqs = args[1:]
	}
	log.Infof("%v", reqs)
	server(args[0], reqs)
	return
    case "client":
	if len(args) < 1 {
	    fmt.Println("uuconn client listen")
	    return
	}
	reqs := []string{}
	if len(args) > 1 {
	    reqs = args[1:]
	}
	log.Infof("%v", reqs)
	client(args[0], reqs)
	return
    }
}
