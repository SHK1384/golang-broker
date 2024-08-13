package main

import (
	"context"
	"github.com/gammazero/workerpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	pb "therealbroker/api/proto"
)

func main() {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.NewClient("localhost:8080", opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewBrokerClient(conn)

	//var wg sync.WaitGroup
	//for i := 0; i < 1000000; i++ {
	//	wg.Add(1)
	//	go func() {
	//		defer wg.Done()
	//		_, err := client.Publish(context.Background(), &pb.PublishRequest{Body: "world", Subject: "ali", ExpirationSeconds: 0})
	//		if err != nil {
	//			panic(err)
	//		}
	//	}()
	//}
	//wg.Wait()

	wp := workerpool.New(10000)
	for i := 0; i < 1000000; i++ {
		wp.Submit(func() {
			_, err = client.Publish(context.Background(), &pb.PublishRequest{Body: "world", Subject: "ali", ExpirationSeconds: 0})
			if err != nil {
				panic(err)
			}
		})
	}
	wp.StopWait()
	//ticker := time.NewTicker(5 * time.Second)
	//timer := time.NewTimer(15 * time.Second)
	//
	//condition := true
	//for condition {
	//	select {
	//	case <-timer.C:
	//		condition = false
	//	case <-ticker.C:
	//		workerPool.SubmitJob(func() {
	//			_, err = client.Publish(context.Background(), &pb.PublishRequest{Body: "world", Subject: "ali", ExpirationSeconds: 0})
	//			if err != nil {
	//				panic(err)
	//			}
	//		})
	//	}
	//}

	//for i := 0; i < 1000; i++ {
	//	_, err = client.Publish(context.Background(), &pb.PublishRequest{Body: "world", Subject: "ali", ExpirationSeconds: 0})
	//	if err != nil {
	//		panic(err)
	//	}
	//}
	//cnt := 0
	//for i := 0; i < 5000; i++ {
	//	_, err := client.Publish(context.Background(), &pb.PublishRequest{Body: []byte("world"), Subject: "ali", ExpirationSeconds: 0})
	//	if err != nil {
	//		panic(err)
	//	}
	//	cnt++
	//}
	//log.Println(cnt)

	//stream, err := client.Subscribe(context.Background(), &pb.SubscribeRequest{Subject: "ali"})
	//if err != nil {
	//	panic(err)
	//}
	//_, err = client.Publish(context.Background(), &pb.PublishRequest{Body: []byte("hello world"), Subject: "ali", ExpirationSeconds: 0})
	//if err != nil {
	//	panic(err)
	//}
	//for {
	//	message, err := stream.Recv()
	//	if err == io.EOF {
	//		break
	//	}
	//	if err != nil {
	//		panic(err)
	//	}
	//	log.Println(string(message.GetBody()))
	//	log.Println("ok")
	//}
	//_, _ = client.Publish(context.Background(), &pb.PublishRequest{Body: []byte("i'm awesome"), ExpirationSeconds: 10})
	//body, _ := client.Fetch(context.Background(), &pb.FetchRequest{Id: int32(40)})
	//fmt.Println(string(body.GetBody()))
}
