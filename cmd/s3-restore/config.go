package main

import (
	"flag"
	"log"
)

// ServiceConfig defines all of the service configuration parameters
type ServiceConfig struct {
	BucketName  string
	KeyName     string
	RestoreDays int
}

// LoadConfiguration will load the service configuration from env/cmdline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {

	var cfg ServiceConfig

	flag.StringVar(&cfg.BucketName, "bucket", "", "The bucket name")
	flag.StringVar(&cfg.KeyName, "key", "", "The key name")
	flag.IntVar(&cfg.RestoreDays, "days", 1, "Number of days to restore (default 1)")

	flag.Parse()

	if len(cfg.BucketName) == 0 {
		log.Fatalf("The bucket name cannot be blank")
	}

	if len(cfg.KeyName) == 0 {
		log.Fatalf("The key name cannot be blank")
	}

	return &cfg
}

//
// end of file
//
