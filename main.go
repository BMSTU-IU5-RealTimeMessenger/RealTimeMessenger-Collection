package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

type Segment struct {
	Data   string `json:"data"`
	Time   int64  `json:"time"`
	Number int    `json:"number"`
	Count  int    `json:"count"`
}

type SegmentWithError struct {
	SegmentData Segment `json:"segment" binding:"required"`
	ErrorSign   *bool    `json:"error" binding:"required"`
}

type Server struct {
	HTTPClient    *http.Client
	KafkaProducer sarama.SyncProducer
	Topic         string
}

func NewServer() (*Server, error) {
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file")
	}

	kafkaAddr := os.Getenv("KAFKA_ADDR")
	topic := os.Getenv("TOPIC")

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{kafkaAddr}, config)
	if err != nil {
		return nil, err
	}

	return &Server{
		HTTPClient:    http.DefaultClient,
		KafkaProducer: producer,
		Topic:         topic,
	}, nil
}

func (s *Server) Run() {
	r := gin.Default()
	r.POST("/transfer", s.Transfer)

	port := os.Getenv("COLLECTION_SERVER_PORT")
	log.Println("Server is running")
	r.Run(os.Getenv("IP") + ":" + port)
}

func (s *Server) Transfer(c *gin.Context) {
	var segment SegmentWithError
	if err := c.ShouldBindJSON(&segment); err != nil {
		log.Println(err)
		c.Status(http.StatusBadRequest)
		return
	}

	bytes, err := json.Marshal(segment.SegmentData)
	if err != nil {
		c.Status(http.StatusInternalServerError)
		return
	}
	data := string(bytes)
	log.Println(">", data)
	msg := &sarama.ProducerMessage{Topic: s.Topic,
		Partition: 0,
		Value:     sarama.StringEncoder(data)}
	_, _, err = s.KafkaProducer.SendMessage(msg)
	if err != nil {
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	c.Status(http.StatusOK)
}

func main() {
	server, err := NewServer()
	if err != nil {
		log.Fatalln(err)
		return
	}

	server.Run()
}
