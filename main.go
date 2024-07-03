package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const mongouri = "mongodb://localhost:27017/api"
const kafkabootstrapservers = "192.168.1.201"
const mongodatabase = "spog_development"
const mongocollection = "alertsources"

func main() {
	fmt.Println("\n\x1b[32mStarting Alert API Server.....\x1b[0m\n")
	
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkabootstrapservers})
	if err != nil {
		fmt.Println("Kafka failed")
		panic(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
					fmt.Println("\n\x1b[33m**************************************************************************\x1b[0m\n")
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
					fmt.Println("\n\x1b[33m**************************************************************************\x1b[0m\n")
				}
			}
		}
	}()

	fmt.Println("\x1b[32mStarting mongo connection.....\x1b[0m\n")

 	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(mongouri).SetServerAPIOptions(serverAPI)
	// Create a new client and connect to the server
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()
	// Send a ping to confirm a successful connection
	var result bson.M
	if err := client.Database("admin").RunCommand(context.TODO(), bson.D{{Key: "ping", Value: 1}}).Decode(&result); err != nil {
		panic(err)
	}
	fmt.Println("\x1b[32mPinged your deployment. You successfully connected to MongoDB!\x1b[0m\n ")
	fmt.Println("\x1b[32mWaiting for alerts.....\x1b[0m\n")

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		Handler(w, r, client , p )
	 })
	http.ListenAndServe(":8082", nil)
}

func Handler(w http.ResponseWriter, r *http.Request, client *mongo.Client , p *kafka.Producer) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading body: %v", err)
		http.Error(w, "can't read body", http.StatusBadRequest)
		return
	}
	intKey := r.URL.Query().Get("Api-Token")
	fmt.Println("GET params were:", r.URL.Query())
	coll1 := client.Database(mongodatabase).Collection(mongocollection)
	filter := bson.D{{ Key: "alertsourcekey", Value: intKey }}
	fmt.Println("The IntKey is " , intKey)

	type Error struct{
		Errorstatus bool `json:"error"`
		Message  string `json:"message"`
	}

	result := bson.M{}
	opts := options.FindOne()
	err1 := coll1.FindOne(context.TODO(), filter, opts).Decode(& result)
	
	if err1 != nil {
		fmt.Println("DB error is ",err1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(&Error{true, "Unknown Client" })
		return
	}


    if intKey == result["alertsourcekey"] {
		fmt.Println("The Source is " , result["alertsourcename"])
		headers := []kafka.Header{
			{Key: "AlertKey", Value: []byte(result["alertsourcekey"].(string))},
		}
		fmt.Println(string(body))
		if IsValidJSON(string(body)){
			topic := "Events"
			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic},
				Value:          body,
				Headers:        headers,
			}, nil)
			w.WriteHeader(http.StatusCreated)
			w.Write(body)
		}else{
			fmt.Println("Bad Request")
			w.WriteHeader(http.StatusBadRequest)
			w.Write(body)
		}
	}else{
		fmt.Println("Unauthorized")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write(body)
	}
}

func IsValidJSON(str string) bool {
    var js json.RawMessage
    return json.Unmarshal([]byte(str), &js) == nil
}
