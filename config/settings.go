package config

import (
  "fmt"
  "log"
  "encoding/json"
  "bytes"
  "time"
  "github.com/jmoiron/sqlx"
  "github.com/lib/pq"

)

var createTrigger = `
BEGIN;
DROP TRIGGER IF EXISTS result_notify_event on tasks_taskresult
CREATE TRIGGER result_notify_event
AFTER INSERT OR UPDATE OR DELETE on tasks_taskresult
  FOR EACH ROW
  EXECUTE PROCEDURE notify_event(%d);
`

var createProcedure = `
CREATE OR REPLACE FUNCTION notify_event()
RETURNS TRIGGER AS $$
  DECLARE
      subscribed_user_id integer;
      data json;
      notification json;
      result json;
  BEGIN
      SELECT json_agg(tmp)
      INTO data
      FROM (
        SELECT * FROM tasks_taskresult
        WHERE tasks_taskresult.user_id = subscribed_user_id
      ) tmp;

      result := json_build_object('data', data, 'row', row_to_json(NEW));

      PERFORM pg_notify('events', result::text);
      RETURN NEW;
  END;
  $$ LANGUAGE plpgsql;
`

const (
  host     = "postgres"
  port     = 5432
  user     = "debug"
  password = "debug"
  dbname   = "shigoto_q"
)

func waitForNotification(l *pq.Listener) {
  for {
    select {
    case n:= <-l.Notify:
      fmt.Println("Received data from channel [", n.Channel, "]:")
      var prettyJSON bytes.Buffer
      err := json.Indent(&prettyJSON, []byte(n.Extra), "", "\t")
      if err != nil {
        log.Printf("Error processing JSON: %s", err)
        return
      }
      fmt.Println(string(prettyJSON.Bytes()))
      return
    case <-time.After(90 * time.Second):
      log.Println("Received no events for 90 seconds, checking connection")
      go func() {
        l.Ping()
      }()
      return
    }
  }
}


func SetupDb() *sqlx.DB {
  psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
    "password=%s dbname=%s sslmode=disable",
    host, port, user, password, dbname)
    log.Println("Connecting to database")
    db, err := sqlx.Connect("postgres", psqlInfo)
    if err != nil {
        log.Fatalln(err)
    }
    reportProblem := func(ev pq.ListenerEventType, err error) {
      if err != nil {
        fmt.Println(err.Error())
      }
    }

    listener := pq.NewListener(psqlInfo, 10*time.Second, time.Minute, reportProblem)
    err = listener.Listen("events")
    if err != nil {
      panic(err)
    }
    fmt.Println("Start monitoring results")
    for {
      waitForNotification(listener)
    }
    return db
}
