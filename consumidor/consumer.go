//de una vez jala todo el batch

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {

	//configuracion inicial
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "topic_test", 0)

	if err != nil {
		fmt.Printf(err.Error())
	}

	//configuracion de tiempo
	//conn.SetWriteDeadline(time.Now().Add(time.Second * 8))
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	//configuracion de lectura de mensajes ingresados a kafka
	batch := conn.ReadBatch(0, 1e6) //1e3=10kb, 1e6 = 1mb max

	//       bytes := make([]byte, 1e6) //ojo con el tam 1e9 no lo soporta xd

	for {
		bytes := make([]byte, 1e3) //ojo con el tam 1e9 no lo soporta xd
		_, err := batch.Read(bytes)
		if err == nil {
			fmt.Println("Msg:", string(bytes))
			time.Sleep(1 * time.Second)

		} else { //error
			break
		}
		//fmt.Println("Msg:",string(bytes))

	}

	//validaciones del repo oficial

	if err := batch.Close(); err != nil {
		log.Fatal("failed to close batch:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close connection:", err)
	}
}
