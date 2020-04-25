package service

import (
	"../proto"
	"../util"
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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

type DriverLocationVersion struct {
	ID        primitive.ObjectID `json:"id" bson:"_id"`
	UID       string
	TimeStamp string
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
	reqUser := User{
		UID:       user.Uid,
		Email:     user.Email,
		Name:      user.Name,
		LastLogin: user.Lastlogin,
	}

	insertResult, err := UserCollection.InsertOne(context.TODO(), reqUser)
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

	filter := bson.M{"uid": bson.M{"$eq": request.Uid}}
	update := bson.M{"$set": bson.M{
		"uid":       request.Uid,
		"timestamp": request.Timestamp,
		"location":  NewPoint(request.Lng, request.Lat),
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

	location := NewPoint(request.SourceLng, request.SourceLat)
	var results []DriverLocation
	filter := bson.D{
		{"location",
			bson.D{
				{"$near", bson.D{
					{"$geometry", location},
					{"$maxDistance", 50000},
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

	driverDetails := results[0]
	return &proto.DriverDetails{
		Uid:        driverDetails.UID,
		Email:      "email",
		Name:       "name",
		DriverLat:  driverDetails.Location.Coordinates[1],
		DriverLong: driverDetails.Location.Coordinates[0],
	}, nil
}

func NewPoint(lng, lat float64) Location {
	return Location{
		Type:        "Point",
		Coordinates: []float64{lng, lat},
	}
}

func AddDriverLocation(driverLocation DriverLocation) interface{} {
	insertResult, err := DriverLocationCollection.InsertOne(context.Background(), driverLocation)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Inserted new Point. ID: %s\n", insertResult.InsertedID)
	return insertResult.InsertedID
}

func UpdateDriverLocation() {
	//filter := bson.D{{"uid", request.Uid}}
	//update := bson.D{
	//	{"$inc", bson.D{
	//		{"coordinates", [2]float64{request.Lng, request.Lat}}}},
	//}
	//
	//updateResult, err := DriverLocationCollection.UpdateOne(context.TODO(), filter, update)
	//if err != nil {
	//	return nil, err
	//}
	//
	//fmt.Printf("Matched %v documents and updated %v documents.\n", updateResult.MatchedCount, updateResult.ModifiedCount)
}
