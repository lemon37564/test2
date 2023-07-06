package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	token  = "sqb2BwqCGQfm8W_4bW3Usx94hv758K0v4NDdhfESdfSP60PXtCbTnpUcIqLK6X54P3-U55MpSUhWdGxdi4P9Bg=="
	bucket = "bucket"
	org    = "org"
)

type Server struct {
	server   *http.Server
	dbClient influxdb2.Client
	logFile  *os.File
}

func NewServer() (s *Server) {
	s.logFile, _ = os.OpenFile(
		"log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0664,
	)
	multi := zerolog.MultiLevelWriter(zerolog.ConsoleWriter{Out: os.Stderr}, s.logFile)
	log.Logger = zerolog.New(multi).With().Timestamp().Logger()

	if v, e := os.LookupEnv("TOKEN"); e {
		token = v
	}
	if v, e := os.LookupEnv("BUCKET"); e {
		bucket = v
	}
	if v, e := os.LookupEnv("ORG"); e {
		org = v
	}

	router := gin.Default()
	router.GET("/hello", func(ctx *gin.Context) { ctx.String(http.StatusOK, "Hello, there.") })
	router.POST("/data", s.postData)
	router.GET("/data", s.getData)
	router.GET("/data/count", s.dataCount)
	router.POST("/restart", func(ctx *gin.Context) { s.Restart() })

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	s.server = &http.Server{
		Addr:    ":" + port,
		Handler: router,
	}

	options := influxdb2.DefaultOptions()
	options.SetHTTPRequestTimeout(1<<32 - 1)
	s.dbClient = influxdb2.NewClientWithOptions("http://192.168.106.228:8086", token, options)

	return s
}

func (s *Server) Start() {
	go func() {
		if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Panic().Msg(err.Error())
		}
	}()
	log.Info().Msg("Server started on port " + s.server.Addr)
}

func (s *Server) Shutdown() {
	// The context is used to inform the server it has 5 seconds to finish
	// the remained requests that are currently handling
	log.Info().Msg("Shutting down the server...")
	s.dbClient.Close()
	s.logFile.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.server.Shutdown(ctx); err != nil {
		log.Fatal().Msg("Server forced to shutdown: " + err.Error() + ".")
	}
	log.Info().Msg("Server closed.")
}

func (s *Server) Restart() {
	s.Shutdown()
	path, err := os.Executable()
	if err != nil {
		log.Error().Msg("Error geting os.Executable: " + err.Error())
	}
	err = syscall.Exec(path, []string{}, []string{})
	if err != nil {
		log.Error().Msg("Error running syscall.Exec: " + err.Error())
	}
}

func (s *Server) postData(ctx *gin.Context) {
	writeAPI := s.dbClient.WriteAPIBlocking(org, bucket)
	tags := map[string]string{
		"tagname1": "tagvalue1",
	}
	fields := map[string]interface{}{
		"field1": rand.Intn(1000) + 1000,
	}
	point := write.NewPoint("measurement1", tags, fields, time.Now())
	if err := writeAPI.WritePoint(context.Background(), point); err != nil {
		log.Error().Msg(err.Error())
	}
}

func (s *Server) getData(ctx *gin.Context) {
	durationString, ex := ctx.GetQuery("range")
	duration, err := strconv.Atoi(durationString)
	if !ex || err != nil {
		duration = 60 * 60 // one hour
	}

	queryAPI := s.dbClient.QueryAPI(org)
	query := `from(bucket: "bucket")
            |> range(start: -%ds)
            |> filter(fn: (r) => r._measurement == "measurement1")`
	query = fmt.Sprintf(query, duration)

	results, err := queryAPI.Query(context.Background(), query)
	if err != nil {
		log.Error().Msg(err.Error())
	}
	ctx.Stream(func(w io.Writer) bool {
		for results.Next() {
			fmt.Fprintln(w, results.Record().String())
			return true
		}
		return false
	})
	if err := results.Err(); err != nil {
		log.Error().Msg(err.Error())
	}
}

func (s *Server) dataCount(ctx *gin.Context) {
	durationString, ex := ctx.GetQuery("range")
	duration, err := strconv.Atoi(durationString)
	if !ex || err != nil {
		duration = 60 * 60 // one hour
	}

	queryAPI := s.dbClient.QueryAPI(org)
	query := `from(bucket: "bucket")
			|> range(start: -%ds)
			|> group()  
			|> count()`
	query = fmt.Sprintf(query, duration)

	results, err := queryAPI.Query(context.Background(), query)
	if err != nil {
		log.Error().Msg(err.Error())
	}

	if results.Next() {
		ctx.String(http.StatusOK, fmt.Sprint(results.Record().Value()))
	} else {
		ctx.Status(http.StatusNoContent)
	}

	if err := results.Err(); err != nil {
		log.Error().Msg(err.Error())
	}
}

func main() {
	server := NewServer()
	server.Start()
	c := make(chan os.Signal, 1)
	defer close(c)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	// block at here until receive signals
	sig := <-c
	log.Info().Msg("Recived signal: " + sig.String())
	server.Shutdown()
}
