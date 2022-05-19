package main

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// taken from the examples here: https://github.com/aws/aws-sdk-go/blob/main/service/s3/examples_test.go

//
// main entry point
//
func main() {

	cfg := LoadConfiguration()

	_ = getObject(cfg)
}

func getObject(cfg *ServiceConfig) error {
	log.Printf("Getting: s3://%s/%s", cfg.BucketName, cfg.KeyName)

	svc := s3.New(session.New())
	input := &s3.GetObjectInput{
		Bucket: aws.String(cfg.BucketName),
		Key:    aws.String(cfg.KeyName),
	}

	result, err := svc.GetObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				log.Printf("ERROR: bucket does not exist (%s)", aerr.Error())
			case s3.ErrCodeNoSuchKey:
				log.Printf("ERROR: key does not exist (%s)", aerr.Error())
			case s3.ErrCodeInvalidObjectState:
				log.Printf("ERROR: inappropriate storage class for get (%s)", aerr.Error())
			default:
				log.Printf("ERROR: %s", aerr.Error())
			}
			return aerr
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			log.Printf("ERROR: %s", err.Error())
			return err
		}
	} else {
		fmt.Printf("INFO: %s", result)
	}
	return nil
}

func restoreObject(cfg *ServiceConfig) error {
	log.Printf("Restoring: s3://%s/%s", cfg.BucketName, cfg.KeyName)

	svc := s3.New(session.New())
	input := &s3.RestoreObjectInput{
		Bucket: aws.String(cfg.BucketName),
		Key:    aws.String(cfg.KeyName),
		RestoreRequest: &s3.RestoreRequest{
			Days: aws.Int64(int64(cfg.RestoreDays)),
			GlacierJobParameters: &s3.GlacierJobParameters{
				Tier: aws.String("Standard"),
			},
		},
	}

	result, err := svc.RestoreObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeObjectAlreadyInActiveTierError:
				log.Printf("ERROR: already restored (%s)", aerr.Error())
			case s3.ErrCodeInvalidObjectState:
				log.Printf("ERROR: inappropriate storage class for restore (%s)", aerr.Error())
			case s3.ErrCodeNoSuchBucket:
				log.Printf("ERROR: bucket does not exist (%s)", aerr.Error())
			case s3.ErrCodeNoSuchKey:
				log.Printf("ERROR: key does not exist (%s)", aerr.Error())
			default:
				log.Printf("ERROR: %s", aerr.Error())
			}
			return aerr
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			log.Printf("ERROR: %s", err.Error())
			return err
		}
	} else {
		fmt.Printf("INFO: %s", result)
	}
	return nil
}

//
// end of file
//
