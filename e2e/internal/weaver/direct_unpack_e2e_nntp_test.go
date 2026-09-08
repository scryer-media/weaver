package weaver

import (
	"bufio"
	"bytes"
	"encoding/xml"
	"fmt"
	"hash/crc32"
	"net"
	"net/textproto"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// A protocol fixture with an explicit availability barrier. Delaying an article
// by a fixed sleep cannot prove extraction overlaps downloading on a fast host.
type unpackArticle struct {
	body    []byte
	gate    *unpackGate
	missing bool
}

type unpackGate struct {
	released chan struct{}
	held     atomic.Int64
}

type unpackNNTP struct {
	listener    net.Listener
	mu          sync.Mutex
	articles    map[string]unpackArticle
	connections map[net.Conn]bool
	stopped     bool
	wg          sync.WaitGroup
	done        chan struct{}
}

func startUnpackNNTP(t *testing.T) *unpackNNTP {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &unpackNNTP{listener: l, articles: map[string]unpackArticle{}, connections: map[net.Conn]bool{}, done: make(chan struct{})}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			if s.stopped {
				s.mu.Unlock()
				conn.Close()
				return
			}
			s.connections[conn] = true
			s.wg.Add(1)
			s.mu.Unlock()
			go s.serve(conn)
		}
	}()
	t.Cleanup(func() {
		s.mu.Lock()
		s.stopped = true
		close(s.done)
		l.Close()
		for conn := range s.connections {
			conn.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
	return s
}

func (s *unpackNNTP) serve(conn net.Conn) {
	defer s.wg.Done()
	defer func() { conn.Close(); s.mu.Lock(); delete(s.connections, conn); s.mu.Unlock() }()
	r := textproto.NewReader(bufio.NewReader(conn))
	w := textproto.NewWriter(bufio.NewWriter(conn))
	if w.PrintfLine("200 direct-unpack fixture ready") != nil {
		return
	}
	for {
		line, err := r.ReadLine()
		if err != nil {
			return
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			return
		}
		switch strings.ToUpper(fields[0]) {
		case "AUTHINFO":
			if len(fields) < 2 {
				err = w.PrintfLine("501 credentials required")
				break
			}
			if strings.EqualFold(fields[1], "USER") {
				err = w.PrintfLine("381 password required")
			} else {
				err = w.PrintfLine("281 authenticated")
			}
		case "CAPABILITIES":
			err = w.PrintfLine("101 capabilities\r\nVERSION 2\r\nREADER\r\n.")
		case "MODE":
			err = w.PrintfLine("200 reader mode")
		case "QUIT":
			w.PrintfLine("205 bye")
			return
		case "BODY", "ARTICLE", "STAT":
			if len(fields) != 2 {
				err = w.PrintfLine("501 message id required")
				break
			}
			id := strings.Trim(fields[1], "<>")
			s.mu.Lock()
			article, ok := s.articles[id]
			s.mu.Unlock()
			// STAT must not wait behind BODY requests: it only reports presence.
			if ok && article.gate != nil && fields[0] != "STAT" {
				article.gate.held.Add(1)
				select {
				case <-article.gate.released:
				case <-s.done:
					article.gate.held.Add(-1)
					return
				}
				article.gate.held.Add(-1)
			}
			if !ok || article.missing {
				err = w.PrintfLine("430 no such article")
				break
			}
			if fields[0] == "STAT" {
				err = w.PrintfLine("223 0 <%s>", id)
				break
			}
			code := 222
			if fields[0] == "ARTICLE" {
				code = 220
			}
			if err = w.PrintfLine("%d 0 <%s>", code, id); err != nil {
				return
			}
			dot := w.DotWriter()
			if code == 220 {
				_, err = fmt.Fprintf(dot, "Message-ID: <%s>\n\n", id)
			}
			if err == nil {
				_, err = dot.Write(article.body)
			}
			if closeErr := dot.Close(); err == nil {
				err = closeErr
			}
		default:
			err = w.PrintfLine("500 unsupported command")
		}
		if err != nil {
			return
		}
	}
}

func unpackYenc(name string, data []byte, part, total, begin, size int, wholeCRC uint32) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "=ybegin part=%d total=%d line=128 size=%d name=%s\n=ypart begin=%d end=%d\n", part, total, size, name, begin, begin+len(data)-1)
	column := 0
	for _, v := range data {
		v += 42
		if v == 0 || v == 10 || v == 13 || v == 61 {
			b.WriteByte('=')
			v += 64
			column++
		}
		b.WriteByte(v)
		column++
		if column >= 128 {
			b.WriteByte('\n')
			column = 0
		}
	}
	if column > 0 {
		b.WriteByte('\n')
	}
	fmt.Fprintf(&b, "=yend size=%d part=%d pcrc32=%08x", len(data), part, crc32.ChecksumIEEE(data))
	if part == total {
		fmt.Fprintf(&b, " crc32=%08x", wholeCRC)
	}
	b.WriteByte('\n')
	return b.Bytes()
}

func xmlUnpackText(s string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}
