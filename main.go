package main

import (
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"strconv"
	"time"
)

var peerNodeList [][2]string

func initPeerNodeList() {
	for i := 1; i <= 6; i++ {
		peer := [2]string{strconv.Itoa(i), ":" + strconv.Itoa(6000+i)}
		peerNodeList = append(peerNodeList, peer)
	}
}

func rpcRegister(node *Node) {
	err := rpc.Register(node)
	if err != nil {
		log.Panic(err)
	}
	port := node.Port
	rpc.HandleHTTP()
	err = http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Panic(err)
	}
}

func (node *Node) startNode() {
	for {
		for {
			//成为竞选者
			if node.becomeCandidate() {
				//竞选leader
				if node.electLeader() {
					break
				} else {
					continue
				}
			} else {
				break
			}
		}

		for {
			time.Sleep(time.Duration(1) * time.Second)
			if node.lastHeartBeartTime != 0 && (time.Now().UnixNano()/int64(time.Millisecond)-node.lastHeartBeartTime) > 7000 {
				fmt.Println("[warning]心跳检测超时，重新开始选举")
				node.votedFor = "-1"
				node.NodeState = 0
				node.voteGet = 0
				node.lastHeartBeartTime = 0
				node.currentLeader = "-1"
				break
			}
		}
	}

}

func main() {
	//初始化节点信息表
	initPeerNodeList()
	var id string
	var port string
	fmt.Print("请输入节点编号:")
	fmt.Scanf("%s", &id)
	temp, _ := strconv.Atoi(id)
	port = strconv.Itoa(6000 + temp)
	//创建节点
	node := NewNode(id, port)
	//绑定rpc
	go rpcRegister(node)
	//监听heartBeatchan ，竞选成功开始调用rpc方法同步心跳
	go node.heartBeat()
	//启动节点
	node.startNode()
}
