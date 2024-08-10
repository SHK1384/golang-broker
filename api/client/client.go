package main

import (
	"context"
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

	//ticker := time.NewTicker(10 * time.Second)
	//defer ticker.Stop()
	//var wg sync.WaitGroup
	//for {
	//	select {
	//	case <-ticker.C:
	//		wg.Wait()
	//		return
	//	default:
	//		wg.Add(1)
	//		go func() {
	//			defer wg.Done()
	//			_, err := client.Publish(context.Background(), &pb.PublishRequest{Body: []byte("world"), Subject: "ali", ExpirationSeconds: 0})
	//			if err != nil {
	//				panic(err)
	//			}
	//		}()
	//	}
	//}

	for i := 0; i < 1000000; i++ {
		_, err := client.Publish(context.Background(), &pb.PublishRequest{Body: []byte("world"), Subject: "ali", ExpirationSeconds: 0})
		if err != nil {
			panic(err)
		}
	}

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
