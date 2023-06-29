package main

import (
	"log"
	"os"
	"path"

	"github.com/uvalib/uva-aws-s3-sdk/uva-s3"
)


//
// main entry point
//
func main() {

	cfg := LoadConfiguration()

	s3Svc, err := uva_s3.NewUvaS3(uva_s3.UvaS3Config{Logging: true})
	if err != nil {
		log.Fatalf("ERROR: %s", err.Error())
	}

	obj := uva_s3.NewUvaS3Object(cfg.BucketName, cfg.KeyName)

	o, err := s3Svc.StatObject(obj)
	if err != nil {
		log.Fatalf("ERROR: %s", err.Error())
	}

        filename := path.Base( cfg.KeyName )

	if o.IsGlacier() == false {
		log.Printf("INFO: object NOT in glacier (or is glacier IR), getting it in the normal way")

		err = s3Svc.GetToFile(o, filename)
		if err != nil {
			log.Fatalf("ERROR: %s", err.Error())
		}
		log.Printf("INFO: available as %s", filename)
	} else {
		if o.IsRestoring() == true {
			log.Printf("INFO: object in glacier, restore is IN PROGRESS...")
		} else {
			if o.IsRestored() == true {
				log.Printf("INFO: object in glacier and has been restored")
				err = s3Svc.GetToFile(o, filename)
				if err != nil {
					log.Fatalf("ERROR: %s", err.Error())
				}
				log.Printf("INFO: available as %s", filename)
			} else {
				log.Printf("INFO: object in glacier, beginning a restore...")
				//err = s3Svc.RestoreObject(o, uva_s3.RESTORE_EXPEDITED, int64(cfg.RestoreDays))
				err = s3Svc.RestoreObject(o, uva_s3.RESTORE_STANDARD, int64(cfg.RestoreDays))
				if err != nil {
					log.Fatalf("ERROR: %s", err.Error())
				}
			}
		}
	}
	os.Exit(0)
}

//
// end of file
//
