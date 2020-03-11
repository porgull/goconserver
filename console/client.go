package console

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	neturl "net/url"
	"os"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/xcat2/goconserver/common"
	"golang.org/x/crypto/ssh/terminal"
)

const (
	CLIENT_INTERACTIVE_MODE = iota
	CLIENT_BROADCAST_MODE
)

type ConsoleClient struct {
	host, port string
	origState  *terminal.State
	cr         int
	searcher   *EscapeSearcher
	exit       chan struct{}
	retry      bool
	inputTask  *common.Task
	outputTask *common.Task
	sigio      chan struct{}
	reported   bool // error already reported
	mode       int  // broadcast, interactive
}

func NewConsoleClient(host string, port string, mode int) *ConsoleClient {
	return &ConsoleClient{host: host,
		port:     port,
		exit:     make(chan struct{}, 0),
		retry:    true,
		sigio:    make(chan struct{}, 1),
		reported: false,
		// clientEscape must not be nil
		searcher: NewEscapeSearcher(clientEscape.root),
		mode:     mode,
	}
}

func (c *ConsoleClient) transCr(b []byte, n int) []byte {
	temp := make([]byte, common.BUF_SIZE)
	j := 0
	for i := 0; i < n; i++ {
		ch := b[i]
		if c.cr == 0 {
			if ch == ' ' {
				c.cr = 1
			} else {
				temp[j] = ch
				j++
			}
		} else if c.cr == 1 {
			if ch == '\r' {
				c.cr = 2
			} else {
				temp[j], temp[j+1] = ' ', ch
				j += 2
				c.cr = 0
			}
		} else if c.cr == 2 {
			if ch == '\n' {
				temp[j], temp[j+1], temp[j+2] = ' ', '\r', ch
				j += 3
			} else {
				temp[j] = ch // ignore " \r"
				j++
			}
			c.cr = 0
		}
	}
	if c.cr == 1 {
		c.cr = 0
		temp[j] = ' '
		j++
	}
	return temp[0:j]
}

func (client *ConsoleClient) processClientSession(conn net.Conn, b []byte, n int, node string) error {
	j := 0
	for i := 0; i < n; i++ {
		ch := b[i]
		// NOTE(chenglch): To avoid of the copy of the buffer, control the send buffer with index
		buffered, handler, err := clientEscape.Search(conn, ch, client.searcher)
		if err != nil {
			return err
		}
		// if the character is buffered, send the buf before current character
		if buffered {
			if j < i && conn != nil {
				err = common.Network.SendByteWithLength(conn.(net.Conn), b[j:i])
				if err != nil {
					return err
				}
			}
			j = i + 1
			continue
		}
		if handler != nil {
			err = handler(conn, client, node, ch)
			if err != nil {
				return err
			}
			j = i + 1
		}
	}
	if conn != nil {
		err := common.Network.SendByteWithLength(conn.(net.Conn), b[j:n])
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *ConsoleClient) input(args ...interface{}) {
	b := args[0].([]interface{})[2].([]byte)
	node := args[0].([]interface{})[1].(string)
	conn := args[0].([]interface{})[0].(net.Conn)
	in := int(os.Stdin.Fd())
	n := 0
	err := common.Fcntl(in, syscall.F_SETFL, syscall.O_ASYNC|syscall.O_NONBLOCK)
	if err != nil {
		return
	}
	if runtime.GOOS != "darwin" {
		err = common.Fcntl(in, syscall.F_SETOWN, syscall.Getpid())
		if err != nil {
			return
		}
	}
	select {
	case _, ok := <-c.sigio:
		if !ok {
			return
		}
		for {
			size, err := syscall.Read(in, b[n:])
			if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
				break
			}
			n += size
		}
		if err != nil && err != syscall.EAGAIN && err != syscall.EWOULDBLOCK {
			if c.reported == false {
				fmt.Println(err)
			}
			c.close()
			return
		}
	}
	if n == 0 {
		return
	}
	c.processClientSession(conn.(net.Conn), b, n, node)
}

func (c *ConsoleClient) appendInput(args interface{}) {
	bufChan := args.(chan []byte)
	b := make([]byte, common.BUF_SIZE)
	in := int(os.Stdin.Fd())
	n := 0
	err := common.Fcntl(in, syscall.F_SETFL, syscall.O_ASYNC|syscall.O_NONBLOCK)
	if err != nil {
		return
	}
	if runtime.GOOS != "darwin" {
		err = common.Fcntl(in, syscall.F_SETOWN, syscall.Getpid())
		if err != nil {
			return
		}
	}
	select {
	case _, ok := <-c.sigio:
		if !ok {
			return
		}
		for {
			size, err := syscall.Read(in, b[n:])
			if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
				break
			}
			n += size
		}
		if err != nil && err != syscall.EAGAIN && err != syscall.EWOULDBLOCK {
			if c.reported == false {
				fmt.Println(err)
			}
			c.close()
			return
		}
	}
	if n == 0 {
		return
	}
	bufChan <- b[:n]
}

func (c *ConsoleClient) output(args ...interface{}) {
	b := args[0].([]interface{})[1].([]byte)
	conn := args[0].([]interface{})[0].(net.Conn)
	n, err := common.Network.ReceiveInt(conn)
	if err != nil {
		if c.retry == true && c.reported == false {
			printConsoleReceiveErr(err)
		}
		c.close()
		return
	}
	b, err = common.Network.ReceiveBytes(conn, n)
	if err != nil {
		if c.retry == true && c.reported == false {
			printConsoleReceiveErr(err)
		}
		c.close()
		return
	}
	b = c.transCr(b, n)
	n = len(b)
	for n > 0 {
		tmp, err := os.Stdout.Write(b)
		if err != nil {
			if pathErr, ok := err.(*os.PathError); ok {
				if pathErr.Err == syscall.EAGAIN || pathErr.Err == syscall.EWOULDBLOCK {
					continue
				}
			}
			if c.retry == true && c.reported == false {
				printConsoleSendErr(err)
			}
			c.close()
			return
		}
		n -= tmp
	}
}

func (c *ConsoleClient) contains(cmds []byte, cmd byte) bool {
	for _, v := range cmds {
		if v == cmd {
			return true
		}
	}
	return false
}

func (c *ConsoleClient) transport(conn net.Conn, node string) error {
	defer conn.Close()
	var err error
	recvBuf := make([]byte, common.BUF_SIZE)
	sendBuf := make([]byte, common.BUF_SIZE)
	if !terminal.IsTerminal(int(os.Stdin.Fd())) {
		return common.ErrNotTerminal
	}
	c.origState, err = terminal.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return err
	}
	defer terminal.Restore(int(os.Stdin.Fd()), c.origState)
	c.inputTask, err = common.GetTaskManager().RegisterLoop(c.input, conn, node, sendBuf)
	if err != nil {
		return err
	}
	defer common.GetTaskManager().Stop(c.inputTask.GetID())
	printConsoleHelpPrompt()
	c.outputTask, err = common.GetTaskManager().RegisterLoop(c.output, conn, recvBuf)
	if err != nil {
		return err
	}
	defer common.GetTaskManager().Stop(c.outputTask.GetID())
	select {
	case <-c.exit:
		break
	}
	return nil
}

func (c *ConsoleClient) Connect() (net.Conn, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%s", c.host, c.port))
	if err != nil {
		printFatalErr(err)
		os.Exit(1)
	}
	clientConfig := common.GetClientConfig()
	clientTimeout := time.Duration(clientConfig.ConsoleTimeout)
	conn, err := net.DialTimeout("tcp", tcpAddr.String(), clientTimeout*time.Second)
	if err != nil {
		printFatalErr(err)
		os.Exit(1)
	}
	err = conn.(*net.TCPConn).SetKeepAlive(true)
	if err != nil {
		printFatalErr(err)
		os.Exit(1)
	}
	err = conn.(*net.TCPConn).SetKeepAlivePeriod(30 * time.Second)
	if err != nil {
		printFatalErr(err)
		os.Exit(1)
	}
	if clientConfig.SSLCertFile != "" && clientConfig.SSLKeyFile != "" && clientConfig.SSLCACertFile != "" {
		tlsConfig, err := common.LoadClientTlsConfig(
			clientConfig.SSLCertFile,
			clientConfig.SSLKeyFile,
			clientConfig.SSLCACertFile,
			c.host,
			clientConfig.Insecure)
		if err != nil {
			panic(err)
		}
		conn = tls.Client(conn, tlsConfig)
		err = conn.(*tls.Conn).Handshake()
		if err != nil {
			return nil, err
		}
	}
	return conn, nil
}

func (c *ConsoleClient) close() {
	c.reported = true
	common.SafeClose(c.exit)
	common.SafeClose(c.sigio)
}

func (c *ConsoleClient) registerSignal(done <-chan struct{}) {
	exitHandler := func(s os.Signal, arg interface{}) {
		fmt.Fprintf(os.Stderr, "handle signal: %v\n", s)
		terminal.Restore(int(os.Stdin.Fd()), c.origState)
		os.Exit(1)
	}
	ioHandler := func(s os.Signal, arg interface{}) {
		common.SafeSend(c.sigio, struct{}{})
	}
	signalSet := common.GetSignalSet()
	signalSet.Register(syscall.SIGINT, exitHandler)
	signalSet.Register(syscall.SIGTERM, exitHandler)
	signalSet.Register(syscall.SIGHUP, exitHandler)
	signalSet.Register(syscall.SIGIO, ioHandler)
	go common.DoSignal(done)
}

type CongoClient struct {
	client  *common.HttpClient
	baseUrl string
}

func NewCongoClient(baseUrl string) *CongoClient {
	baseUrl = strings.TrimSuffix(baseUrl, "/")
	clientConfig := common.GetClientConfig()
	httpClient := http.Client{Timeout: time.Second * 5}
	client := &common.HttpClient{Client: &httpClient, Headers: http.Header{}}
	if strings.HasPrefix(baseUrl, "https") && clientConfig.SSLKeyFile != "" &&
		clientConfig.SSLCertFile != "" && clientConfig.SSLCACertFile != "" {
		tlsConfig, err := common.LoadClientTlsConfig(
			clientConfig.SSLCertFile,
			clientConfig.SSLKeyFile,
			clientConfig.SSLCACertFile,
			clientConfig.ServerHost,
			clientConfig.Insecure)
		if err != nil {
			panic(err)
		}
		client.Client.Transport = &http.Transport{TLSClientConfig: tlsConfig}
	}
	return &CongoClient{client: client, baseUrl: baseUrl}
}

func (c *CongoClient) List() ([]interface{}, error) {
	url := fmt.Sprintf("%s/nodes", c.baseUrl)
	var nodes []interface{}
	ret, err := c.client.Get(url, nil, nil, false)
	if err != nil {
		return nodes, err
	}
	val, ok := ret.(map[string]interface{})["nodes"].([]interface{})
	if !ok {
		return nodes, common.ErrInvalidType
	}
	return val, nil
}

func (c *CongoClient) Show(node string) (interface{}, error) {
	url := fmt.Sprintf("%s/nodes/%s", c.baseUrl, node)
	var ret interface{}
	ret, err := c.client.Get(url, nil, nil, true)
	if err != nil {
		return ret, err
	}
	return ret, nil
}

func (c *CongoClient) Logging(node string, state string) (interface{}, error) {
	url := fmt.Sprintf("%s/nodes/%s", c.baseUrl, node)
	params := neturl.Values{}
	params.Set("state", state)
	var ret interface{}
	ret, err := c.client.Put(url, &params, nil, false)
	if err != nil {
		return ret, err
	}
	return ret, nil
}

func (c *CongoClient) Delete(node string) (interface{}, error) {
	url := fmt.Sprintf("%s/nodes/%s", c.baseUrl, node)
	var ret interface{}
	ret, err := c.client.Delete(url, nil, nil, false)
	if err != nil {
		return ret, err
	}
	return ret, nil
}

func (c *CongoClient) Create(node string, attribs map[string]interface{}, params map[string]interface{}) (interface{}, error) {
	url := fmt.Sprintf("%s/nodes", c.baseUrl)
	data := attribs
	data["params"] = params
	data["name"] = node
	var ret interface{}
	ret, err := c.client.Post(url, nil, data, false)
	if err != nil {
		return ret, err
	}
	return ret, nil
}

func (c *CongoClient) replay(node string) (string, error) {
	url := fmt.Sprintf("%s/command/replay/%s", c.baseUrl, node)
	ret, err := c.client.Get(url, nil, nil, true)
	if err != nil {
		if ret != nil {
			return string(ret.([]byte)), err
		}
		return "", err
	}
	return string(ret.([]byte)), nil
}

func (c *CongoClient) listUser(node string) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/command/user/%s", c.baseUrl, node)
	ret, err := c.client.Get(url, nil, nil, false)
	if err != nil {
		return nil, err
	}
	return ret.(map[string]interface{}), nil
}

func (c *CongoClient) listBreakSequence() ([]string, error) {
	url := fmt.Sprintf("%s/breaksequence", c.baseUrl)
	ret, err := c.client.Get(url, nil, nil, false)
	if err != nil {
		return nil, err
	}
	items := make([]string, 0)
	for _, item := range ret.([]interface{}) {
		items = append(items, item.(string))
	}
	return items, nil
}
