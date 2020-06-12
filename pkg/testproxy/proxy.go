package testproxy

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
)

type T struct {
	FromAddr  string
	ToAddr    string
	Ln        net.Listener
	lastID    uint
	conns     map[uint]net.Conn
	closed    bool
	TLSConfig *tls.Config
}

func (p *T) GetNextID() uint {
	if p.lastID >= ^uint(0) {
		p.lastID = 0
	}
	p.lastID++
	return p.lastID
}

func (p *T) Run() error {
	var (
		ln  net.Listener
		err error
	)
	p.closed = false
	p.lastID = 0
	p.conns = map[uint]net.Conn{}

	if p.TLSConfig != nil {
		fmt.Printf("Test Kafka Proxy listening with TLS at %s\n", p.FromAddr)
		ln, err = tls.Listen("tcp", p.FromAddr, p.TLSConfig)
	} else {
		ln, err = net.Listen("tcp", p.FromAddr)
	}
	if err != nil {
		return err
	}
	p.Ln = ln

	go func() {
		for {
			if p.closed {
				break
			}
			conn, err := ln.Accept()
			if err != nil {
				fmt.Printf("Could not accept connection: %v\n", err)
				return
			}
			p.conns[p.GetNextID()] = conn
			go p.handleConnection(conn)
		}
	}()
	return nil
}

func (p *T) handleConnection(c net.Conn) {
	fmt.Printf("Connection FROM: %s; Connecting TO: %s\n", c.RemoteAddr(), p.ToAddr)
	dstConn, err := net.Dial("tcp", p.ToAddr)
	if err != nil {
		fmt.Printf("Error connecting to destination: %v\n", err)
		return
	}
	go func() {
		for {
			data := make([]byte, 1024)
			n, err := c.Read(data)
			if n > 0 {
				_, err = dstConn.Write(data[:n])
				if err != nil {
					fmt.Printf("Error writing to dst connection: %v\n", err)
					return
				}
			}
			if err != nil {
				if err != io.EOF {
					fmt.Printf("Error reading from client connection: %v\n", err)
				}
				_ = dstConn.Close()
				break
			}
		}
	}()
	for {
		data := make([]byte, 1024)
		n, err := dstConn.Read(data)
		if n > 0 {
			_, err = c.Write(data[:n])
			if err != nil {
				fmt.Printf("Error writing to dst connection: %v\n", err)
				return
			}
		}
		if err != nil {
			fmt.Printf("Error reading from dst connection: %v\n", err)
			_ = dstConn.Close()
			break
		}
	}
	_ = dstConn.Close()
}

func (p *T) Close() {
	p.closed = true
	_ = p.Ln.Close()
	for _, c := range p.conns {
		_ = c.Close()
	}
}
