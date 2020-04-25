package main

import (
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
	sw "../service"
	"../proto"
)

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", 9999))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// create a gRPC server object
	grpcServer := grpc.NewServer()

	//creating server instance
	s := sw.Server{}

	sw.CreateClientForMongoDB()
	proto.RegisterDriverServiceServer(grpcServer, &s)

	//start the server
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}

}
