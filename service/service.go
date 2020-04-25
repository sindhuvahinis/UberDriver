package service

import (
	"../proto"
	"../util"
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
)

type User struct {
	UID       string
	Email     string
	Name      string
	LastLogin int64
}

type Location struct {
	UID       string
	Latitude  float64
	Longitude float64
	Timestamp int64
}

type Server struct {
}

var Collection *mongo.Collection

func CreateClientForMongoDB() {
	// Set client options
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")

	// Connect to MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)

	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to MongoDB!")

	Collection = client.Database("Uber").Collection("Users")
}

func (s *Server) StoreUserLogin(ctx context.Context, user *proto.User) (*proto.Response, error) {
	req_user := User{
		UID:       user.Uid,
		Email:     user.Email,
		Name:      user.Name,
		LastLogin: user.Lastlogin,
	}

	insertResult, err := Collection.InsertOne(context.TODO(), req_user)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Inserted a single user record: ", insertResult.InsertedID)

	return &proto.Response{
		StatusCode: util.SUCCESS,
		IsOK:       true,
		Message:    fmt.Sprintf("Inserted user record successfully. Inserted ID is %v", insertResult.InsertedID),
	}, nil
}

func (s *Server) UpdateLocation(ctx context.Context, request *proto.LocationRequest) (*proto.Response, error) {
	location := Location{
		UID:       request.Uid,
		Latitude:  request.Lat,
		Longitude: request.Lng,
		Timestamp: request.Timestamp,
	}

	insertResult, err := Collection.InsertOne(context.TODO(), location)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Insert a location record ", insertResult.InsertedID)

	return &proto.Response{
		StatusCode: util.SUCCESS,
		IsOK:       true,
		Message:    fmt.Sprintf("Inserted location record for the given user id successfully. Inserted ID is %v", insertResult.InsertedID),
	}, nil
}
