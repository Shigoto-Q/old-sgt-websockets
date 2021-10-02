package types

import (
  "github.com/gorilla/websocket"
)

// User subscribtion
type User struct {
  ID string
  Conn *websocket.Conn
}

// Message for publishing
type Message struct {
  DeliveryID string `json:"id"`
  Content string `json:"content"`
}
