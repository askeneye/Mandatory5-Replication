package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	pb "distributed_auction/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

//Lowest allive is the leader
var rmAddrs = []string{"localhost:50051", "localhost:50052", "localhost:50053"}

var input = bufio.NewScanner(os.Stdin)

type bidder struct {
	name string
	rms []pb.AuctionClient
	current int
	request_id int64
}

func (b *bidder) join() {
	for {
		fmt.Print("Enter your username:")
		if !input.Scan() {
			os.Exit(0)
		}
		name := strings.TrimSpace(input.Text())
		if name == "" {
			fmt.Println("Name cannot be an empty string")
			continue
		}

		resp, err := b.sendBid(&pb.BidRequest{Bidder: name})
		if err != nil {
			log.Fatalf("No RM answered, cannot join")
		}
		if resp.GetOutcome() == pb.BidOutcome_SUCCESS {
			b.name = name
			return
		}
		fmt.Println("Could not join:", resp.GetReason())
	}
}

func (b *bidder) nextRM() {
	log.Printf("RM %d did not answer, trying next one", b.current)
	
	//Changing current to next one
	b.current = (b.current + 1) % len(b.rms)

	//Timeout
	time.Sleep(500 * time.Millisecond)
}

func (b *bidder) sendBid(req *pb.BidRequest) (*pb.BidResponse, error) {
	for i := 0; i < len(b.rms); i++ {
		
		ctx, cancel := context.WithTimeout(context.Background(), 3 * time.Second)
		resp, err := b.rms[b.current].Bid(ctx, req)
		cancel()

		if err == nil && resp.GetOutcome() != pb.BidOutcome_EXCEPTION {
			return resp, nil
		}
		b.nextRM()
	}
	return nil, errors.New("no RM answered")
}

func (b *bidder) bid(amount int64) {
	if b.name == "" {
		b.join()
	}

	b.request_id++
	resp, err := b.sendBid(&pb.BidRequest{Bidder: b.name, Bid: amount, RequistId: b.request_id})
	if err != nil {
		fmt.Println("EXCEPTION: no RM answered")
		return
	}
	fmt.Println(resp.GetOutcome(), resp.GetReason())
}

func (b *bidder) result() {
	for i := 0; i < len(b.rms); i++ {
		
		ctx, cancel := context.WithTimeout(context.Background(), 3 * time.Second)
		resp, err := b.rms[b.current].Result(ctx, &pb.ResultRequest{})
		cancel()

		if err == nil {
			switch {
			case resp.GetBidder() == "":
				fmt.Println("No bids yet")
			case resp.GetOver():
				fmt.Printf("Auction is over: %s won with %d\n", resp.GetBidder(), resp.GetAmount())
			default:
				fmt.Printf("Auction is ongoing: highest bid is %d by %s, %d seconds left\n", resp.GetAmount(), resp.GetBidder(), resp.GetSecondsLeft())
			}
			return
		}
		b.nextRM()
	}
	fmt.Println("EXCEPTION: no RM answered")
}


func main() {
	b := &bidder{}

	for _, addr := range rmAddrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Bad address %s: %v", addr, err)
		}
		defer conn.Close()

		b.rms = append(b.rms, pb.NewAuctionClient(conn))
	}

	b.readCommands()
}


func (b *bidder) readCommands() {
	fmt.Println("Enter one of the following actions: bid <amount> | result | quit")
	for input.Scan() {
		
		//Splits up into array
		cmd := strings.Fields(input.Text())
		if len(cmd) == 0 {
			continue
		}

		switch cmd[0] {
		case "bid":
			if len(cmd) != 2 {
				fmt.Println("Usage: bid <amount>")
				continue
			}
			
			amount, err := strconv.ParseInt(cmd[1], 10, 64)
			if err != nil || amount <= 0 {
				fmt.Println("The amount should be a whole number above 0")
			}

			b.bid(amount)
		case "result":
			b.result()
		case "quit":
			return
		default:
			fmt.Println("Commands: bid <amount> | result | quit")
		}
	}
}