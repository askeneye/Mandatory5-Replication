package main

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	pb "distributed_auction/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var rmAddrs = []string{"localhost:50051", "localhost:50052", "localhost:50053"}

type RM struct {
	pb.UnimplementedAuctionServer

	id     int
	leader int
	mu     sync.Mutex
	state  *pb.State

	peerDown []bool
	peers    []pb.AuctionClient
}

func (rm *RM) handleBid(req *pb.BidRequest) *pb.BidResponse {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	name, bid := req.GetBidder(), req.GetBid()
	s := rm.state

	if bid == 0 {
		if s.Bidders[name] {
			return &pb.BidResponse{Outcome: pb.BidOutcome_FAIL, Reason: "Name is taken. Try another"}
		} else {
			s.Bidders[name] = true
			rm.replicate()
			log.Printf("[RM %d] %s joined", rm.id, name)
			return &pb.BidResponse{Outcome: pb.BidOutcome_SUCCESS}
		}
	}

	if !s.Bidders[name] {
		return &pb.BidResponse{Outcome: pb.BidOutcome_FAIL, Reason: "Need to register with name first"}
	}
	if s.LastSucces[name] == req.GetRequistId() {
		return &pb.BidResponse{Outcome: pb.BidOutcome_SUCCESS}
	}
	if time.Now().UnixMilli() > s.EndTime {
		return &pb.BidResponse{Outcome: pb.BidOutcome_FAIL, Reason: "Auction is over"}
	}
	if bid <= s.HighestBid {
		return &pb.BidResponse{Outcome: pb.BidOutcome_FAIL, Reason: "Current bid is higher"}
	}

	s.HighestBid, s.HighestBidder = bid, name
	s.LastSucces[name] = req.GetRequistId()
	rm.replicate()
	log.Printf("[RM %d] new bid is now highest %d by %s", rm.id, s.HighestBid, s.HighestBidder)
	return &pb.BidResponse{Outcome: pb.BidOutcome_SUCCESS}
}

func (rm *RM) Bid(ctx context.Context, req *pb.BidRequest) (*pb.BidResponse, error) {
	for {
		leader := rm.getLeader()
		if leader == rm.id {
			return rm.handleBid(req), nil
		}

		fctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		resp, err := rm.peers[leader].Bid(fctx, req)
		cancel()
		if err == nil {
			return resp, nil
		}
		rm.leaderDied(leader)

	}
}

func (rm *RM) handleResult(req *pb.ResultRequest) *pb.ResultResponse {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	s := rm.state

	if time.Now().UnixMilli() > s.EndTime {
		return &pb.ResultResponse{
			Over:   true,
			Bidder: s.HighestBidder,
			Amount: s.HighestBid,
		}
	} else {
		timeLeft := s.EndTime - time.Now().UnixMilli()
		return &pb.ResultResponse{
			Over:        false,
			Bidder:      s.HighestBidder,
			Amount:      s.HighestBid,
			SecondsLeft: max(0, (timeLeft / 1000)),
		}
	}

}

func (rm *RM) Result(ctx context.Context, req *pb.ResultRequest) (*pb.ResultResponse, error) {
	for {
		leader := rm.getLeader()
		if leader == rm.id {
			return rm.handleResult(req), nil
		}

		fctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		resp, err := rm.peers[leader].Result(fctx, req)
		cancel()
		if err == nil {
			return resp, nil
		}
		rm.leaderDied(leader)
	}
}

func (rm *RM) getLeader() int {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.leader
}

func (rm *RM) leaderDied(leader int) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.leader == leader {
		rm.leader = leader + 1
		log.Printf("[RM %d] RM %d is down, new leader is RM %d", rm.id, leader, rm.leader)
	}
}

func (rm *RM) replicate() {
	for id, peer := range rm.peers {
		if id == rm.id {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := peer.Update(ctx, rm.state)
		cancel()

		if err != nil && !rm.peerDown[id] {
			log.Printf("[RM %d] RM is not answering %d %v", rm.id, id)
		}
		rm.peerDown[id] = err != nil
	}
}

func (rm *RM) Update(ctx context.Context, in *pb.State) (*pb.Empty, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if in.Bidders == nil {
		in.Bidders = map[string]bool{}
	}
	if in.LastSucces == nil {
		in.LastSucces = map[string]int64{}
	}
	rm.state = in
	log.Printf("[RM %d] has been updated: %s leads with %d", rm.id, rm.state.HighestBidder, rm.state.HighestBid)
	return &pb.Empty{}, nil
}

func main() {

	rm := &RM{
		peerDown: make([]bool, len(rmAddrs)),
		state: &pb.State{
			EndTime:    time.Now().Add(100 * time.Second).UnixMilli(),
			Bidders:    map[string]bool{},
			LastSucces: map[string]int64{},
		}}

	var listener net.Listener

	for id, addr := range rmAddrs {
		lis, err := net.Listen("tcp", addr)

		if err == nil {
			listener, rm.id = lis, id
			break
		}
	}

	if listener == nil {
		log.Fatalf("[RM] all addresses are taken")
	}

	for _, addr := range rmAddrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatal(err)
		}
		rm.peers = append(rm.peers, pb.NewAuctionClient(conn))
	}

	go func() {
		for range time.Tick(time.Second) {
			if rm.getLeader() == rm.id {
				rm.mu.Lock()
				rm.replicate()
				rm.mu.Unlock()
			}
		}
	}()

	s := grpc.NewServer()
	pb.RegisterAuctionServer(s, rm)
	log.Printf("[Rm %d] listening on %s", rm.id, rmAddrs[rm.id])
	log.Fatalln(s.Serve(listener))
}
