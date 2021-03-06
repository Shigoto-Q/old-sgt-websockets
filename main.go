package main

import (
  "net/http"
  "context"
  "fmt"
  "log"
  "sync"
  "github.com/gorilla/websocket"
  "github.com/gomodule/redigo/redis"
  "github.com/satori/go.uuid"
  "github.com/Shigoto/sgt-websockets/config"
  "github.com/Shigoto/sgt-websockets/types"
)


var ctx = context.Background()

type User struct {
  ID string
  conn *websocket.Conn
}

type Store struct {
  Users []*types.User
  sync.Mutex
}

type Message struct {
  DeliveryID string `json:"id"`
  Content string `json:"content"`
}

var (
  gStore *Store
  gPubSubConn *redis.PubSubConn
  gRedisConn = func() (redis.Conn, error){
    return redis.Dial("tcp", "redis:6379")
  }
)

func init(){
  gStore = &Store{
    Users: make([]*types.User, 0, 1),
  }
}

func (s *Store) newUser(conn *websocket.Conn) *types.User {
  u := &types.User{
    ID: uuid.NewV4().String(),
    Conn: conn,
  }
  if err := gPubSubConn.Subscribe(u.ID); err != nil {
    panic(err)
  }
  s.Lock()
  defer s.Unlock()
  s.Users = append(s.Users, u)
  return u
}

func deliverMessages() {
  for {
    switch v:= gPubSubConn.Receive().(type) {
    case redis.Message:
      gStore.findAndDeliver(v.Channel, string(v.Data))

    case redis.Subscription:
      log.Printf("Subscription message : %s: %s %d\n", v.Channel, v.Kind, v.Count)

    case error:
      log.Println("Error pub/sub, delivery stopped")
      return
    }
  }
}

func (s *Store) findAndDeliver(userID, content string) {
  m := Message{
    Content: content,
  }

  for _, u:= range s.Users{
    if u.ID == userID {
      if err := u.Conn.WriteJSON(m); err != nil {
        log.Printf("Error on message delivery e: %s\n", err)
      } else {
        log.Printf("User %s found, message sent\n", userID)
      }
      return
    }
  }
  log.Printf("User %s not found in our store\n.", userID)
}

func publishResult(channel string, message string, conn *redis.PubSubConn) {
  defer conn.Conn.Close()
  conn.Conn.Do("PUBLISH", channel, message)
}

var serverAddress = ":8080"

func main() {
  var db = config.SetupDb()
  log.Print(db)
  gRedisConn, err := gRedisConn()
  if err != nil {
    panic(err)
  }
  defer gRedisConn.Close()
  gPubSubConn = &redis.PubSubConn{Conn: gRedisConn}
  defer gPubSubConn.Close()
  go deliverMessages()
  http.HandleFunc("/ws", wsHandler)
  log.Printf("server started at %s\n", serverAddress)
  log.Fatal(http.ListenAndServe(serverAddress, nil))
}

var upgrader = websocket.Upgrader{
  CheckOrigin: func(r *http.Request) bool {
    return true
  },
}


const (
  host     = "postgres"
  port     = 5432
  user     = "debug"
  password = "debug"
  dbname   = "shigoto_q"
)

func wsHandler(w http.ResponseWriter, r *http.Request) {
  psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
    "password=%s dbname=%s sslmode=disable",
    host, port, user, password, dbname)
  conn, err := upgrader.Upgrade(w, r, nil)
  if err != nil {
    log.Printf("upgrader error %s\n" + err.Error())
    return
  }
  u := gStore.newUser(conn)
  log.Printf("user %s has connected\n", u.ID)
  for {
    var m Message
    if err := u.Conn.ReadJSON(&m); err != nil {
      log.Printf("error on websocket. message: %s\n", err)
    }
    if c, err := gRedisConn(); err != nil {
      log.Printf("Error on redis connection. %s\n", err)
    } else {
      config.ListenEvents(psqlInfo, c, u)
      c.Do("PUBLISH", m.DeliveryID, string(m.Content))
    }
  }
}
