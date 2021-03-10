package main

import (
	"fmt"
	"math/rand"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

type Node struct {
	ID             string
	Port           string
	voteGet        int
	lock           sync.Mutex
	votedFor       string
	currentNodeNum int
	//0 跟随者 1 候选人 2 领导
	NodeState          int
	lastHeartBeartTime int64
	currentLeader      string
	timeout            int
	voteCh             chan bool
	heartBeatCh        chan bool
}

//初始化node
func NewNode(id, port string) *Node {
	node := new(Node)
	node.ID = id
	node.voteGet = 0
	node.Port = port
	node.votedFor = "-1"
	node.currentNodeNum = 1
	node.NodeState = 0
	node.lastHeartBeartTime = 0
	node.currentLeader = "-1"
	node.voteCh = make(chan bool)
	node.heartBeatCh = make(chan bool)
	return node
}

//等待150-300ms
func getWaitTime() int {
	return rand.Intn(3000) + 1500
}

//获取当前在线节点数量
func (node *Node) getCurrentNodeNum() int {
	onlineNum := 0
	fmt.Println("当前节点状态信息:")
	for _, peerNode := range peerNodeList {
		if peerNode[0] == node.ID {
			onlineNum++
			fmt.Println("节点 " + node.ID + " 在线")
			continue
		}
		_, err := rpc.DialHTTP("tcp", "127.0.0.1"+peerNode[1])
		if err != nil {
			fmt.Println("节点 " + peerNode[0] + " 离线")
			continue
		}
		fmt.Println("节点 " + peerNode[0] + " 在线")
		onlineNum++
	}
	fmt.Println("")
	return onlineNum

}

func (node *Node) becomeCandidate() bool {
	//获取一个3-4.5秒的等待时间
	b := getWaitTime()
	time.Sleep(time.Duration(b) * time.Millisecond)
	//判断是否是是follower，当前是否不存在leader，是否已经投票
	if node.NodeState == 0 && node.currentLeader == "-1" && node.votedFor == "-1" {
		//成为竞选者，为自己投票
		node.NodeState = 1
		node.votedFor = node.ID
		node.voteGet += 1
		fmt.Println("[info]节点 " + node.ID + " 成为候选人")
		return true
	} else {
		return false
	}
}

func (node *Node) electLeader() bool {
	fmt.Println("[info]节点 " + node.ID + " 开始进行Leader选举")
	//通过rcp方法让其他节点为自己投票
	go node.rpcMessage("Node.Vote", node)
	for {
		//监听chan
		select {
		//超时转为跟随者
		case <-time.After(time.Duration(3) * time.Second):
			fmt.Println("[warning]选举超时，转变为跟随者状态")
			node.NodeState = 0
			node.voteGet = 0
			node.votedFor = "-1"
			return false
		//	监听选票chan
		case ok := <-node.voteCh:
			if ok {
				node.voteGet += 1
				fmt.Printf("获取一张选票，当前票数：%d\n", node.voteGet)
			}
			//获取当前在线节点数
			node.currentNodeNum = node.getCurrentNodeNum()
			if node.voteGet > node.currentNodeNum/2 && node.currentLeader == "-1" {
				fmt.Println("[success]当前票数 " + strconv.Itoa(node.voteGet) + " 竞选Leader成功，已获得半数以上票数")
				node.NodeState = 2
				node.currentLeader = node.ID
				//通过rpc方法通知其他节点当前leader
				go node.rpcMessage("Node.ElectionConfirm", node)
				node.heartBeatCh <- true
				return true
			}
		}
	}
}

//rpc方法，得到其他成为leader信息
func (node *Node) ElectionConfirm(n Node, res *bool) error {
	node.currentLeader = n.ID
	fmt.Println("[info]节点 " + n.ID + " 竞选为leader")
	node.voteGet = 0
	node.votedFor = "-1"
	node.NodeState = 0
	*res = true
	return nil
}


func (node *Node) heartBeat() {
	//等待heartBeatCh得到true
	if <-node.heartBeatCh {
		for {
			//调用rpc方法发送心跳包
			node.rpcMessage("Node.HeartBeatResponse", node)
			node.lastHeartBeartTime = time.Now().UnixNano() / int64(time.Millisecond)
			time.Sleep(time.Second * time.Duration(3))
		}
	}
}

//rpc方法，处理收到的心跳
func (node *Node) HeartBeatResponse(n Node, res *bool) error {
	node.currentLeader = n.ID
	node.lastHeartBeartTime = time.Now().UnixNano() / int64(time.Millisecond)
	fmt.Println("[info]接收到领导节点 " + n.ID + " 的心跳消息\n")
	*res = true
	return nil
}

//rpc方法，为其他节点投票
func (node *Node) Vote(n Node, res *bool) error {
	if node.votedFor != "-1" || node.currentLeader != "-1" {
		*res = false
	} else {
		node.votedFor = n.ID
		node.NodeState = 0
		*res = true
	}
	return nil
}

//rpc方法，用于接收客服端发来的消息
func (node *Node) ReceiveClientMessage(message string, resMessage *string) error {
	if node.currentLeader == node.ID {
		fmt.Println("[info]已收到用户发来的消息, 内容为: " + message + " \n正在转发消息....")
		node.rpcMessage("Node.ReceiveMessage", message)
		* resMessage = "已收到用户发来的消息,转发完毕"
	} else {
		*resMessage = "notLeader"
	}
	return nil
}

//rpc方法，用于同步leader的消息
func (node *Node) ReceiveMessage(message string, resMessage *bool) error {
	fmt.Println("[info]接收到Leader同步的用户消息\n内容为: " + message)
	*resMessage = true
	return nil
}

//rpc方法，用于客户端获取当前leader
func (node *Node) GetCurrentLeadr(message string, LeaderId *string) error {
	*LeaderId = node.currentLeader
	return nil
}

//处理rpc请求
func (node *Node) rpcMessage(method string, args interface{}) {
	for _, peerNode := range peerNodeList {
		if peerNode[0] == node.ID {
			continue
		}
		rp, err := rpc.DialHTTP("tcp", "127.0.0.1"+peerNode[1])
		if err != nil {
			continue
		}
		var result = false
		err = rp.Call(method, args, &result)
		if err != nil {
			fmt.Println("[warning]远程调用失败")
			continue
		}
		if result {
			switch method {
			case "Node.Vote":
				node.voteCh <- result
			case "Node.HeartBeatResponse":
				fmt.Println("[info]收到节点 " + peerNode[0] + " 心跳确认信息")
			case "Node.ElectionConfirm":
				fmt.Println("[info]收到节点 " + peerNode[0] + " 同步竞选成功信息")
			case "Node.ReceiveMessage":
				fmt.Println("[info]收到节点" + peerNode[0] + "用户消息同步确认信息")
			}
		}
	}
	fmt.Println("")
}
