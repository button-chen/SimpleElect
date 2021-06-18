package elect

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	EventAddPeer = iota
	EventDelPeer
	EventElectLeader
	EventAddKV
	EventDelKV
)

const (
	CmdNotify = 101
)

const (
	ModeRelease = iota
	ModeDebug
)

type IpVal struct {
	ip   string
	val  uint64
	port int
}

func (iv IpVal) String() string {
	return fmt.Sprintf("IP:%s Port: %d", iv.ip, iv.port)
}

type Command struct {
	Ty      int
	Key     string
	Val     string
	Expired int
	Tm      time.Time
}

func (c Command) String() string {
	return fmt.Sprintf("key:%s val: %s tm: %s", c.Key, c.Val, c.Tm.Format("2006-01-02 15:04:05"))
}

type value struct {
	Val     string
	Expired int
	Tm      time.Time
	off     chan struct{}
}

type heartInfo struct {
	tm        time.Time
	joinElect bool
}

var LOG = log.Println
var DBG = log.Println

type EventCallback func(code int, data interface{})

type Elect interface {
	Init(localAddr string, membersAddr []string)
	Start()
	Get(key string) (string, error)
	Set(key, val string, expired int) error
	Local() string
	Leader() string
	AddListenEvent(fun EventCallback)
	SetMode(mode int)
}

type SimpleElect struct {
	IpS              []IpVal
	KeyV             sync.Map // map[string]value
	MyIPVal          IpVal
	heartActive      sync.Map // map[IpVal]heartInfo
	eventCb          []EventCallback
	expiredCheckChan chan func() bool
	leader           IpVal
	minExpired       int    // default 5000ms
	minActive        int    // default 300ms
	joinElect        bool
	done             chan struct{}
}

func NewSimpleElect() Elect {
	return &SimpleElect{
		IpS:              make([]IpVal, 0),
		eventCb:          make([]EventCallback, 0),
		done:             make(chan struct{}),
		expiredCheckChan: make(chan func() bool, 30000),
		minExpired:       5000,
		minActive:        300,
	}
}

func (eh *SimpleElect) Init(localAddr string, membersAddr []string) {
	for _, s := range membersAddr {
		t := strings.Split(s, ":")
		item := IpVal{
			ip:   t[0],
			val:  0,
			port: 56000,
		}
		if len(t) > 1 {
			item.port, _ = strconv.Atoi(t[1])
		}
		item.val = eh.unique(item.ip, item.port)
		eh.IpS = append(eh.IpS, item)
		if localAddr != "" && localAddr == s {
			eh.MyIPVal = item
		}
	}
	sort.Slice(eh.IpS, func(i, j int) bool {
		return eh.IpS[i].val > eh.IpS[j].val
	})

	if localAddr == "" {
		eh.MyIPVal = eh.localIpValue()
	}

	DBG("members:", eh.IpS)
	DBG("self:", eh.MyIPVal)

	http.HandleFunc("/elect/heart", eh.heart)
	http.HandleFunc("/elect/load", eh.load)
	http.HandleFunc("/elect/store", eh.store)

	go func() {
		DBG("listen http on: ", eh.MyIPVal.port)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", eh.MyIPVal.port), nil); err != nil {
			LOG("listen http failed: ", &eh.MyIPVal.port)
		}
	}()
}

func (eh *SimpleElect) SetMode(mode int) {
	if mode == ModeRelease {
		DBG = func(v ...interface{}) {}
	}
}

func (eh *SimpleElect) Start() {
	go eh.loop()
}

func (eh *SimpleElect) Stop() {
	close(eh.done)
}

func (eh *SimpleElect) Get(key string) (string, error) {
	data, err := eh.httpGet("load", eh.leader, "key="+key)
	if err != nil {
		return "", err
	}
	v := value{}
	json.Unmarshal(data, &v)
	return v.Val, nil
}

func (eh *SimpleElect) Set(key, val string, expired int) error {
	if expired != 0 && expired < eh.minExpired {
		expired = eh.minExpired
	}
	cmd := Command{
		Key:     key,
		Val:     val,
		Expired: expired,
	}
	msg, _ := json.Marshal(cmd)
	_, err := eh.httpPost("store", string(msg), eh.leader)
	if err == nil {
		return nil
	}
	return errors.New("not leader")
}

func (eh *SimpleElect) Local() string {
	return eh.leader.ip
}

func (eh *SimpleElect) Leader() string {
	return eh.leader.ip
}

func (eh *SimpleElect) AddListenEvent(fun EventCallback) {
	eh.eventCb = append(eh.eventCb, fun)
}

func (eh *SimpleElect) active(peer IpVal) bool {
	info, ok := eh.heartActive.Load(peer)
	if ok {
		dis := int(time.Now().Sub(info.(heartInfo).tm).Milliseconds())
		if dis < eh.minActive {
			return true
		}
	}
	return false
}

func (eh *SimpleElect) isLeader() bool {
	return eh.leader.val == eh.MyIPVal.val
}

func (eh *SimpleElect) tickerHeart(pr IpVal) {
	d, err := eh.httpGet("heart", pr, "")
	if err == nil {
		if t, ok := eh.heartActive.Load(pr); !ok || t.(heartInfo).tm.IsZero() {
			DBG("member online:", pr.ip, pr.port)
			for _, fun := range eh.eventCb {
				fun(EventAddPeer, pr)
			}
		}
		eh.heartActive.Store(pr, heartInfo{
			tm:        time.Now(),
			joinElect: string(d) == "true",
		})
		return
	}
	if t, ok := eh.heartActive.Load(pr); !ok || !t.(heartInfo).tm.IsZero() {
		DBG("member offline:", pr.ip, pr.port)
		for _, fun := range eh.eventCb {
			fun(EventDelPeer, pr)
		}
	}
	eh.heartActive.Store(pr, heartInfo{
		tm:        time.Time{},
		joinElect: false,
	})
}

func (eh *SimpleElect) heartLoop() {
	for _, peer := range eh.IpS {
		go func(pr IpVal) {
			ticker := time.NewTicker(time.Millisecond * 50)
			for {
				select {
				case <-ticker.C:
					eh.tickerHeart(pr)
				case <-eh.done:
					return
				}
			}
		}(peer)
	}
}

func (eh *SimpleElect) loop() {
	go eh.heartLoop()
	go eh.eventLoop()
	go eh.expiredCheckLoop()
}

func (eh *SimpleElect) expiredCheckLoop() {
	for f := range eh.expiredCheckChan {
		if f() {
			continue
		}
		eh.expiredCheckChan <- f
	}
}

func (eh *SimpleElect) eventLoop() {

	first := true
	ticker := time.NewTicker(time.Millisecond * 100)
	for {
		select {
		case <-ticker.C:
			for _, peer := range eh.IpS {
				t, ok := eh.heartActive.Load(peer)
				if !ok {
					continue
				}
				if !t.(heartInfo).joinElect {
					continue
				}
				if int(time.Now().Sub(t.(heartInfo).tm).Milliseconds()) > eh.minActive {
					continue
				}
				if eh.leader.val != peer.val {
					eh.leader = peer
					for _, fun := range eh.eventCb {
						fun(EventElectLeader, eh.leader)
					}
					DBG("current leader: ", peer.ip, peer.port)
				}
				break
			}
			if !eh.active(eh.leader) || eh.leader.ip == "" {
				eh.joinElect = true
				DBG("join elect ")
			}

			if first && eh.leader.ip != "" && eh.active(eh.leader) {
				err := eh.sync()
				if err == nil {
					first = false
				}
			}

		case <-eh.done:
			return

		} // end select
	}
}

func (eh *SimpleElect) hasIp(ip string) bool {
	addrList, _ := net.InterfaceAddrs()
	for _, addr := range addrList {
		ipNet, ok := addr.(*net.IPNet)
		if !ok || ipNet.IP.To4() == nil {
			continue
		}
		if ipNet.IP.String() != ip {
			continue
		}
		return true
	}
	return false
}

func (eh *SimpleElect) localIpValue() IpVal {
	for _, ipVal := range eh.IpS {
		if !eh.hasIp(ipVal.ip) {
			continue
		}
		return ipVal
	}
	panic("ip config error")
	return IpVal{}
}

func (eh *SimpleElect) unique(ip string, port int) uint64 {

	var ipVal uint32
	ipb := net.ParseIP(ip).To4()
	ipVal |= uint32(ipb[0]) << 24
	ipVal |= uint32(ipb[1]) << 16
	ipVal |= uint32(ipb[2]) << 8
	ipVal |= uint32(ipb[3])

	uni := (uint64(ipVal) << 16) | uint64(port)
	return uni
}

func (eh *SimpleElect) heart(w http.ResponseWriter, r *http.Request) {
	txt := "false"
	if eh.joinElect {
		txt = "true"
	}
	w.Write([]byte(txt))
}

func (eh *SimpleElect) proxy2Leader(w http.ResponseWriter, r *http.Request) {
	remote, _ := url.Parse(fmt.Sprintf("http://%s:%d", eh.leader.ip, eh.leader.port))
	proxy := httputil.NewSingleHostReverseProxy(remote)
	proxy.ServeHTTP(w, r)
}

func (eh *SimpleElect) sync() error {
	data, err := eh.httpGet("load", eh.leader, "")
	if err != nil {
		return err
	}
	tmp := make(map[string]value)
	json.Unmarshal(data, &tmp)
	for k, v := range tmp {
		v.off = make(chan struct{})
		eh.KeyV.Store(k, v)
		eh.expiredCheckChan <- eh.timeout(k, v)
	}
	return nil
}

func (eh *SimpleElect) load(w http.ResponseWriter, r *http.Request) {
	search := r.URL.Query().Get("search")
	if search == "ok" {
		goto handle
	}
	if !eh.isLeader() {
		eh.proxy2Leader(w, r)
		return
	}

handle:
	key := r.URL.Query().Get("key")
	if key == "" {
		tmp := make(map[string]value)
		eh.KeyV.Range(func(key, val interface{}) bool {
			tmp[key.(string)] = val.(value)
			return true
		})
		data, _ := json.Marshal(tmp)
		w.Write(data)
		return
	}
	if v, ok := eh.KeyV.Load(key); ok {
		t, _ := json.Marshal(v.(value))
		w.Write(t)
		return
	}
	if !eh.isLeader() {
		return
	}

	vTmp := make([]value, 0)
	for _, peer := range eh.IpS {
		if peer.val == eh.MyIPVal.val {
			continue
		}
		if !eh.active(peer) {
			continue
		}
		data, err := eh.httpGet("load", peer, "search=ok&key="+key)
		if err != nil || string(data) == "" {
			continue
		}
		v := value{}
		json.Unmarshal(data, &v)
		vTmp = append(vTmp, v)
	}
	sort.Slice(vTmp, func(i, j int) bool {
		return vTmp[i].Tm.UnixNano() > vTmp[j].Tm.UnixNano()
	})
	if len(vTmp) > 0 {
		eh.KeyV.Store(key, vTmp[0])
		go eh.notify("store", Command{
			Key:     key,
			Val:     vTmp[0].Val,
			Expired: vTmp[0].Expired,
			Tm:      vTmp[0].Tm,
		})
		t, _ := json.Marshal(vTmp[0])
		w.Write(t)
	}
	return
}

func (eh *SimpleElect) getReqCmd(r *http.Request) (Command, error) {
	body, err := ioutil.ReadAll(r.Body)
	defer func() {
		r.Body = ioutil.NopCloser(bytes.NewReader(body))
	}()
	if err != nil {
		return Command{}, err
	}
	var cmd Command
	err = json.Unmarshal(body, &cmd)
	if err != nil {
		return Command{}, err
	}
	cmd.Tm = time.Now()
	return cmd, nil
}

func (eh *SimpleElect) store(w http.ResponseWriter, r *http.Request) {

	cmd, err := eh.getReqCmd(r)
	if err != nil {
		w.Write([]byte("store param error: " + err.Error()))
		return
	}
	if cmd.Ty == CmdNotify {
		goto handle
	}
	if !eh.isLeader() {
		eh.proxy2Leader(w, r)
		return
	}

	eh.notify("store", cmd)

handle:
	if v, ok := eh.KeyV.Load(cmd.Key); ok {
		close(v.(value).off)
	}
	val := value{
		Val:     cmd.Val,
		Expired: cmd.Expired,
		Tm:      cmd.Tm,
		off:     make(chan struct{}),
	}
	eh.KeyV.Store(cmd.Key, val)
	if val.Expired != 0 {
		eh.expiredCheckChan <- eh.timeout(cmd.Key, val)
	}
	for _, fun := range eh.eventCb {
		fun(EventAddKV, cmd)
	}
	w.Write([]byte("ok"))
}

func (eh *SimpleElect) timeout(k string, v value) func() bool {

	expired := v.Expired - int(time.Now().Sub(v.Tm).Milliseconds())
	if expired <= 0 {
		expired = 10
	}
	timer := time.NewTimer(time.Millisecond * time.Duration(expired))

	return func() bool {
		select {
		case <-timer.C:
			eh.KeyV.Delete(k)
			for _, fun := range eh.eventCb {
				fun(EventDelKV, Command{Key: k, Val: v.Val, Tm: v.Tm})
			}
			return true
		case <-v.off:
			timer.Stop()
			return true
		case <-eh.done:
			return true
		default:
			return false
		} // end select
	}
}

func (eh *SimpleElect) notify(method string, cmd Command) {

	cmd.Ty = CmdNotify
	msg, _ := json.Marshal(cmd)

	wg := sync.WaitGroup{}
	for _, peer := range eh.IpS {
		if peer.val == eh.MyIPVal.val {
			continue
		}
		if !eh.active(peer) {
			DBG("not active ", peer.ip, peer.port)
			continue
		}
		wg.Add(1)
		go func(pr IpVal) {
			defer wg.Done()
			_, err := eh.httpPost(method, string(msg), pr)
			if err != nil {
				DBG("notify update failed: ", pr.ip, pr.port)
			}
		}(peer)
	}
	wg.Wait()
}

func (eh *SimpleElect) httpGet(method string, iv IpVal, param string) ([]byte, error) {

	if param != "" {
		param = "?" + param
	}
	addr := fmt.Sprintf("http://%s:%d/elect/%s%s", iv.ip, iv.port, method, param)

	client := &http.Client{Timeout: time.Second}
	resp, err := client.Get(addr)
	if err != nil {
		return []byte{}, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []byte{}, err
	}
	return body, nil
}

func (eh *SimpleElect) httpPost(method string, param string, iv IpVal) ([]byte, error) {

	addr := fmt.Sprintf("http://%s:%d/elect/%s", iv.ip, iv.port, method)

	client := &http.Client{Timeout: time.Second}
	resp, err := client.Post(addr, "application/json", strings.NewReader(param))
	if err != nil {
		return []byte{}, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []byte{}, err
	}
	return body, nil
}
