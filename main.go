package main

import (
	"log"
	"os"
	"time"

	"github.com/usace-cloud-compute/cc-go-sdk"
	_ "github.com/usace-cloud-compute/consequences-runner/actions"
)

const (
	localData  string = "/app/data"
	pluginName string = "consequences"
)

func main() {
	log.Println(time.Now())
	t := time.Now()
	//make sure a local directory exists
	if _, err := os.Stat(localData); os.IsNotExist(err) {
		os.MkdirAll(localData, 0644) //do i need to trim filename?
	}
	pm, err := cc.InitPluginManager() //.InitPluginManager()
	if err != nil {
		log.Fatalf("Unable to initialize the plugin manager: %s\n", err.Error())
	}
	err = pm.RunActions()
	if err != nil {
		log.Fatalf("Unable to run actions: %s\n", err.Error())
	}
	log.Println(time.Now())
	s := time.Since(t) //Now().Sub(t)
	log.Println(s)
}
