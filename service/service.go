package service

import (
	"../proto"
	"../util"
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"log"
	"time"
)

type User struct {
	UID       string
	Email     string
	Name      string
	LastLogin int64
}

type Location struct {
	Type        string    `json:"type" bson:"type"`
	Coordinates []float64 `json:"coordinates" bson:"coordinates"`
}

type DriverLocation struct {
	UID       string
	TimeStamp int64
	Location  Location
}

type Server struct {
}

var UserCollection *mongo.Collection
var DriverLocationCollection *mongo.Collection

var (
	DriverLocationStr = "DriverLocation"
	UserStr           = "Users"
)

func CreateClientForMongoDB() {
	// Set client options
	clientOptions := options.Client().ApplyURI("mongodb://0.tcp.ngrok.io:18579/")

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

	database := client.Database("Uber")

	indexOptions := options.CreateIndexes().SetMaxTime(time.Second * 10)

	driverLocationIndexModel := mongo.IndexModel{
		Options: options.Index().SetBackground(true),
		Keys:    bsonx.MDoc{"location": bsonx.String("2dsphere")},
	}

	DriverLocationCollection = database.Collection(DriverLocationStr)
	UserCollection = database.Collection(UserStr)
	DriverLocationIndexes := DriverLocationCollection.Indexes()
	_, err = DriverLocationIndexes.CreateOne(context.Background(), driverLocationIndexModel, indexOptions)

	if err != nil {
		log.Fatal(err)
	}
}

func (s *Server) StoreUserLogin(ctx context.Context, user *proto.User) (*proto.Response, error) {
	filter := bson.M{"uid": bson.M{"$eq": user.Uid}}
	update := bson.M{"$set": bson.M{
		"uid":       user.Uid,
		"email":     user.Email,
		"name":      user.Name,
		"lastlogin": user.Lastlogin,
	}}

	UpdateOptions := options.Update().SetUpsert(true)

	updatedResult, err := UserCollection.UpdateOne(context.Background(), filter, update, UpdateOptions)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Inserted a single user record: ", updatedResult.UpsertedID)

	return &proto.Response{
		StatusCode: util.SUCCESS,
		IsOK:       true,
		Message:    fmt.Sprintf("Inserted user record successfully. Inserted ID is %v", updatedResult.UpsertedID),
	}, nil
}

func (s *Server) UpdateLocation(ctx context.Context, request *proto.LocationRequest) (*proto.Response, error) {

	filter := bson.M{"uid": bson.M{"$eq": request.Uid}}
	update := bson.M{"$set": bson.M{
		"uid":       request.Uid,
		"timestamp": request.Timestamp,
		"location":  NewLocation(request.Lng, request.Lat),
	}}

	UpdateOptions := options.Update().SetUpsert(true)

	updateResult, _ := DriverLocationCollection.UpdateOne(context.Background(), filter, update, UpdateOptions)

	fmt.Printf("Update new Location. Upserted ID: %v\n", updateResult.UpsertedID)

	return &proto.Response{
		StatusCode: util.SUCCESS,
		IsOK:       true,
		Message:    fmt.Sprintf("Inserted location record for the given user id successfully"),
	}, nil
}

func (s *Server) GetDriverInLocation(ctx context.Context, request *proto.GetLocationRequest) (*proto.DriverDetails, error) {

	location := NewLocation(request.SourceLng, request.SourceLat)
	var results []DriverLocation
	filter := bson.D{
		{"location",
			bson.D{
				{"$near", bson.D{
					{"$geometry", location},
					{"$maxDistance", 50000000},
				}},
			}},
	}

	cur, err := DriverLocationCollection.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	for cur.Next(ctx) {
		var driverLoc DriverLocation
		err := cur.Decode(&driverLoc)
		if err != nil {
			fmt.Println("Could not decode Point")
			return nil, err
		}
		results = append(results, driverLoc)
	}

	fmt.Printf("Results %v ", results[0])
	driverDetails := results[0]

	var user User
	err = UserCollection.FindOne(ctx, bson.M{"uid": driverDetails.UID}).Decode(&user)
	if err != nil {
		return nil, err
	}

	return &proto.DriverDetails{
		Uid:        driverDetails.UID,
		Email:      user.Email,
		Name:       user.Name,
		DriverLat:  driverDetails.Location.Coordinates[1],
		DriverLong: driverDetails.Location.Coordinates[0],
	}, nil
}

func NewLocation(lng, lat float64) Location {
	return Location{
		Type:        "Point",
		Coordinates: []float64{lng, lat},
	}
}
