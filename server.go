package main

import (
  "log"
  "os"
  "fmt"
  "net/http"
  "encoding/json"

  "github.com/joho/godotenv"
  "github.com/labstack/echo"
  "github.com/line/line-bot-sdk-go/linebot"

  "github.com/punneng/echo-kafka-producer-example/producer"
)

type producedMessage struct {
  Id string         `json:"id"`
  Message string `json:"message"`
}

func main() {
  err := godotenv.Load()
  if err != nil {
    log.Fatal("Error loading .env file")
  }

  // Initialize linebot client
  client := &http.Client{}
  bot, err := linebot.New(os.Getenv("CHANNEL_SECRET"), os.Getenv("CHANNEL_ACCESS_TOKEN"), linebot.WithHTTPClient(client))
  if err != nil {
    log.Fatal("Line bot client ERROR: ", err)
  }

  // Initialize kafka producer
  err = producer.InitKafka()
  if err != nil {
    log.Fatal("Kafka producer ERROR: ", err)
  }

  // Initilaze Echo web server
  e := echo.New()

  // just for testing
  e.GET("/", func(c echo.Context) error {
    return c.String(http.StatusOK, "Hello, World!")
  })

  e.POST("/webhook", func(c echo.Context) error {
    events, err := bot.ParseRequest(c.Request())
    if err != nil {
      // Do something when something bad happened.
    }

    topics := "go-event-message"

    for _, event := range events {
      if event.Type == linebot.EventTypeMessage {
        switch message := event.Message.(type) {
        case *linebot.TextMessage:
          messageJson, _ := json.Marshal(&producedMessage{
            Id: message.ID,
            Message: message.Text,
          })
          producerErr := producer.Produce(topics, string(messageJson))
          if producerErr != nil {
            log.Print(err)
          } else {
            messageResponse := fmt.Sprintf("Produced [%s] successfully", string(messageJson))
            if _, err = bot.ReplyMessage(event.ReplyToken, linebot.NewTextMessage(messageResponse)).Do(); err != nil {
              log.Print(err)
            }
          }
        }
      }
    }
    return c.String(http.StatusOK, "OK!")
  })

  e.Logger.Fatal(e.Start(":1323"))
}
