package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	/*
		El paquete gorilla/muximplementa un enrutador de solicitudes y un despachador para hacer coincidir las solicitudes entrantes con su respectivo controlador
	*/
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	/*
		Un puerto Go (golang) del proyecto Ruby dotenv (que carga env vars desde un archivo .env)
	*/)

type Obj2 struct {
	Lista []Obj `json:"lista"`
}

type Obj struct {
	Id   string `json:"id"`
	Data string `json:"data"` //json.RawMessage `json:"data"`
}

func failOnError(err error, msg string) {
	if err != nil {
		fmt.Println(msg)
	}
}

func main() {

	fmt.Println("Backend Corriendo en el puerto 7000....")
	router := mux.NewRouter()
	router.HandleFunc("/create", createUser).Methods("POST")
	router.HandleFunc("/salud", salud).Methods("GET")
	log.Fatal(http.ListenAndServe(":7000", handlers.CORS(handlers.AllowedHeaders([]string{"X-Requested-With", "Content-Type", "Authorization"}), handlers.AllowedMethods([]string{"GET", "POST", "PUT", "HEAD", "OPTIONS"}), handlers.AllowedOrigins([]string{"*"}))(router)))

}

func salud(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Que hay xd")
}

/*
Se ejectuta cuando se visita la URL principal
ENDPOINT entre el balanceador y el microservicio
*/
func createUser(response http.ResponseWriter, request *http.Request) {
	response.Header().Add("content-type", "application/json")
	var usr Obj2
	json.NewDecoder((request.Body)).Decode(&usr) //paso del objetoJson a obj de golang

	//muestro q ya los tengo como objeto-golang
	fmt.Println(">>>>>>>>>>>>>>>>>   ", usr.Lista[0].Id)
	fmt.Println(">>>>>>>>>>>>>>>>>   ", usr.Lista[0].Data)

	//retorno a postman thunder etc
	json.NewEncoder(response).Encode(usr)
}

/*
func indexRoute(w http.ResponseWriter, r *http.Request) {
	//Mensaje que aparece al visitar la ruta
	fmt.Fprintf(w, "Bienvenido a Kafka\n")

	var body map[string]interface{}
	err := json.NewDecoder(r.Body).Decode(&body)
	failOnError(err, "Parsing JSON")
	body["way"] = "Kafka"

	//fmt.Println(body)
	mjson, _ := json.Marshal(body)
	// Publicar el mensaje, convierto el objeto JSON a String
	fmt.Println(">>>>>>>>>>", string(mjson))
	//send_message(string(mjson))

	//Informacion de vuelta que indica que se genero la peticion
	fmt.Fprintf(w, "Mensaje Entregado :)\n")
}*/

/*

func send_message(datos string) {
	//configuracion inicial
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "topic_test", 0)

	if err != nil {
		fmt.Printf(err.Error())
		return
	}

	//configuracion de tiempo
	conn.SetWriteDeadline(time.Now().Add(time.Second * 10))

	//-------------------------------- de []bytes a JSON--------------------
	//-------------------------------- de []bytes a JSON--------------------
	byt := []byte(`{"lista":[{"id":"1","data":"hello world1"},{"id":"2","data":"hello world2"},{"id":"3","data":"hello world3"},{"id":"4","data":"hello world4"}]}`)

	var obj Obj2
	if err := json.Unmarshal(byt, &obj); err != nil {
		panic(err)
	}

	fmt.Println("[]BYTE -> JSON\n", obj)

	//---------------------------------de JSON a []bytes
	b, _ := json.Marshal(obj)
	// Convert bytes to string.
	s := string(b)
	fmt.Println("------------------------------\nJSON -> []BYTE \n", s)

	//----------------------------------------------------------------------

	//configuracion del mensaje a ingresar a kafka
	conn.WriteMessages(kafka.Message{Value: b})
	//	conn.WriteMessages(kafka.Message{Value: []byte("hello world from golang")})

	//[]byte(datos)
}*/
