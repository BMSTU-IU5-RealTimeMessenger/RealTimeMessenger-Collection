package main

import (
	"io"
	"log"
	"net/http"
	"os"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

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
	r.Run(":" + port)
}

func (s *Server) Transfer(c *gin.Context) {
	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	log.Println("Got segmet:")
	log.Println(string(data))
	msg := &sarama.ProducerMessage{Topic: s.Topic,
		Partition: 0,
		Value:     sarama.ByteEncoder(data)}
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
