package bulkCache

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
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
	GiveUpTime int64 = 15 //10 minutes
)

type (
	Dage struct {
		Listener net.Listener
		Clients  []*Client
	}
	Client struct {
		Conn net.Conn
		Last int64 //unix timestamp
	}
)

func NewDage() *Dage {
	return &Dage{
		Clients: []*Client{},
	}
}

func (d *Dage) Listen(port string) {
	Listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal(fmt.Sprintf("Listen dage server on %s error[%s]", port, err.Error()))
	}
	d.Listener = Listener
	go func(listener net.Listener) {
		for {
			Cli, err := listener.Accept()
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
		for _, cli := range d.Clients {
			if now-cli.Last > GiveUpTime {
				// shutdown connection
				cli.Conn.Close()
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
				log.Println(err.Error())
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
	for _, i := range its {
		items = append(items, string(i.Data))
	}
	r := strings.Join(items, "\t\t")
	return []string{r}
}

//params bulkname
//response Success or Failure
func (d *Dage) RemoveCommand(params []string) []string {
	if len(params) != 1 {
		return []string{Failure}
	}
	Default.Remove(params[0])
	return []string{Success}
}

func init() {
	DageApi = NewDage()
}
