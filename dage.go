package bulkCache

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
)

const (
	Ping    = "Ping"
	Pong    = "Pong"
	Set     = "Set"
	GET     = "Get"
	Remove  = "Remove"
	Quit    = "Quit"
	Success = "Success"
	Failure = "Failure"
)

var (
	DageApi    *Dage
	GiveUpTime int64 = 600 //10 minutes
)

type (
	Dage struct {
		Listener net.Listener
		Clients  []*Client
		Log      *log.Entry
	}
	Client struct {
		Conn net.Conn
		Last int64 //unix timestamp
	}

	DageClient struct {
		Conn net.Conn
	}
)

func NewDage() *Dage {
	return &Dage{
		Clients: []*Client{},
		Log: log.WithFields(log.Fields{
			"Api": "Dage protocol",
		}),
	}
}

func NewDageClient() *DageClient {
	return new(DageClient)
}

func (d *Dage) Listen(port string) {
	d.Log.Info(fmt.Sprintf("Start Dage server on %s", port))
	Listener, err := net.Listen("tcp", port)
	if err != nil {
		d.Log.Error(fmt.Sprintf("Listen dage server on %s error[%s]", port, err.Error()))
		os.Exit(1)
	}
	d.Listener = Listener
	go func(listener net.Listener) {
		for {
			Cli, err := listener.Accept()
			d.Log.Info(fmt.Sprintf("Accept a dage client[%s]", Cli.RemoteAddr().String()))
			if err != nil {
				continue
			}
			client := &Client{Conn: Cli, Last: time.Now().Unix()}
			d.Clients = append(d.Clients, client)
			go d.Handle(client)
		}
	}(Listener)
	go d.Heart()
}

func (d *Dage) Heart() {
	for {
		<-time.After(time.Second)
		now := time.Now().Unix()
		for i, cli := range d.Clients {
			if now-cli.Last > GiveUpTime {
				// shutdown connection
				d.Log.Warning(fmt.Sprintf("Dage client %s timeout", cli.Conn.RemoteAddr().String()))
				cli.Conn.Close()
				d.Clients = append(d.Clients[0:i], d.Clients[i+1:]...)
				break
			}
		}
	}
}

func (d *Dage) Handle(cli *Client) {
	s := bufio.NewScanner(cli.Conn)
	for s.Scan() {
		l := s.Text()
		cmd := strings.Split(l, "\t")
		resp := d.Command(cmd, cli)
		if resp != "" {
			_, err := cli.Conn.Write([]byte(resp))
			if err != nil {
				d.Log.Error(fmt.Sprintf("Write data to %s error[%s]", cli.Conn.RemoteAddr().String(), err.Error()))
				return
			}
		}
	}
	//client quit
}

func (d *Dage) Command(cmd []string, cli *Client) string {
	cli.Last = time.Now().Unix()
	resp := []string{}
	if len(cmd) == 0 {
		d.Log.Warning("Invalid protocol")
		return ""
	}
	c := cmd[0]
	switch c {
	case Ping:
		resp = append(resp, Pong)
	case Quit:
		cli.Conn.Write([]byte("Good luck!\n"))
		cli.Conn.Close()
	case Set:
		resp = d.SetCommand(cmd[1:])
	case GET:
		resp = d.GetCommand(cmd[1:])
	case Remove:
		resp = d.RemoveCommand(cmd[1:])
	}
	if len(resp) > 0 {
		resp = append(resp, "\n")
	}
	return strings.Join(resp, " ")
}

//params bulkname key value expire
//response Success or Failure
func (d *Dage) SetCommand(params []string) []string {
	if len(params) != 4 {
		return []string{Failure}
	}
	expire, err := strconv.Atoi(params[3])
	if err != nil {
		return []string{Failure}
	}
	if err := Default.Add(params[0], params[1], []byte(params[2]), time.Duration(expire)*time.Second); err != nil {
		return []string{Failure}
	}
	d.Log.Info(fmt.Sprintf("Add %d bytes to %s", len(params[2]), params[0]))
	return []string{Success}
}

//params bulkname
//response value1 \t value2 \t value3
func (d *Dage) GetCommand(params []string) []string {
	if len(params) != 1 {
		return []string{""}
	}
	its, ok := Default.Get(params[0])
	if !ok {
		return []string{""}
	}
	items := []string{}
	bytes := 0
	for _, i := range its {
		bytes += len(i.Data)
		items = append(items, string(i.Data))
	}
	r := strings.Join(items, "\t\t")
	d.Log.Info(fmt.Sprintf("From Bulk %s Get %d bytes data", params[0], bytes))
	return []string{r}
}

//params bulkname
//response Success or Failure
func (d *Dage) RemoveCommand(params []string) []string {
	if len(params) != 1 {
		return []string{Failure}
	}
	Default.Remove(params[0])
	d.Log.Info(fmt.Sprintf("Deleted Bulk %s", params[0]))
	return []string{Success}
}

func init() {
	DageApi = NewDage()
}
