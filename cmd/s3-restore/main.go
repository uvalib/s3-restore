package main

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"os"
	"strings"
)

type S3Object struct {
	BucketName  string // the bucket
	KeyName     string // the key
	IsGlacier   bool   // is the object stored in glacier
	IsRestoring bool   // is the object currently being restored
	Restored    bool   // has the object been restored
	Size        int64  // object size
}

// taken from the examples here: https://github.com/aws/aws-sdk-go/blob/main/service/s3/examples_test.go

//
// main entry point
//
func main() {

	cfg := LoadConfiguration()
	svc := s3.New(session.New())

	object, err := headObject(cfg, svc)
	if err != nil {
		os.Exit(1)
	}

	if object.IsGlacier == false {
		log.Printf("INFO: object NOT in glacier, getting it in the normal way")
		err = getObject(svc, object)
		if err != nil {
			os.Exit(1)
		}
	} else {
		if object.IsRestoring == true {
			log.Printf("INFO: object in glacier, restore is IN PROGRESS...")
		} else {
			if object.Restored == true {
				log.Printf("INFO: object in glacier and has been restored")
				err = getObject(svc, object)
				if err != nil {
					os.Exit(1)
				}
			} else {
				log.Printf("INFO: object in glacier, beginning a restore...")
				err = restoreObject(cfg, svc, object)
				if err != nil {
					os.Exit(1)
				}
			}
		}
	}
	os.Exit(0)
}

func headObject(cfg *ServiceConfig, svc *s3.S3) (S3Object, error) {
	log.Printf("Head: s3://%s/%s", cfg.BucketName, cfg.KeyName)

	object := S3Object{BucketName: cfg.BucketName, KeyName: cfg.KeyName}

	input := &s3.HeadObjectInput{
		Bucket: aws.String(cfg.BucketName),
		Key:    aws.String(cfg.KeyName),
	}

	result, err := svc.HeadObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "BadRequest":
				log.Printf("ERROR: bucket does not exist (%s)", aerr.Error())
			case "NotFound":
				log.Printf("ERROR: key does not exist (%s)", aerr.Error())
			default:
				log.Printf("ERROR: %s (%s)", aerr.Code(), aerr.Error())
			}
			return object, aerr
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			log.Printf("ERROR: %s", err.Error())
			return object, err
		}
	} else {
		//log.Printf("INFO: %s", result)
	}

	// get object attributes
	object.IsGlacier = result.StorageClass != nil && strings.HasPrefix(*result.StorageClass, "GLACIER")
	object.IsRestoring = result.Restore != nil && strings.HasPrefix(*result.Restore, "ongoing-request=\"true\"")
	object.Restored = result.Restore != nil && strings.HasPrefix(*result.Restore, "ongoing-request=\"false\"")
	object.Size = *result.ContentLength
	return object, nil
}

func getObject(svc *s3.S3, object S3Object) error {
	log.Printf("Getting: s3://%s/%s", object.BucketName, object.KeyName)

	input := &s3.GetObjectInput{
		Bucket: aws.String(object.BucketName),
		Key:    aws.String(object.KeyName),
		Range:  aws.String(fmt.Sprintf("bytes=0-%d", object.Size-1)),
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
				log.Printf("ERROR: %s (%s)", aerr.Code(), aerr.Error())
			}
			return aerr
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			log.Printf("ERROR: %s", err.Error())
			return err
		}
	} else {
		//log.Printf("INFO: %s", result)

		filename := "downloaded.file"
		file, err := os.Create(filename)
		if err != nil {
			return err
		}
		defer file.Close()
		buf := new(bytes.Buffer)
		buf.ReadFrom(result.Body)
		file.WriteString(buf.String())
		log.Printf("INFO: downloaded as %s", filename)
	}
	return nil
}

func restoreObject(cfg *ServiceConfig, svc *s3.S3, object S3Object) error {
	log.Printf("Restoring: s3://%s/%s", object.BucketName, object.KeyName)

	input := &s3.RestoreObjectInput{
		Bucket: aws.String(object.BucketName),
		Key:    aws.String(object.KeyName),
		RestoreRequest: &s3.RestoreRequest{
			Days: aws.Int64(int64(cfg.RestoreDays)),
			GlacierJobParameters: &s3.GlacierJobParameters{
				Tier: aws.String("Standard"),
			},
		},
	}

	_, err := svc.RestoreObject(input)
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
				log.Printf("ERROR: %s (%s)", aerr.Code(), aerr.Error())
			}
			return aerr
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			log.Printf("ERROR: %s", err.Error())
			return err
		}
	} else {
		//log.Printf("INFO: %s", result)
	}
	return nil
}

//
// end of file
//
