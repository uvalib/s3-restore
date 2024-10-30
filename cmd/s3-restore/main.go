package main

import (
	"log"
	"os"
	"path"

	"github.com/uvalib/uva-aws-s3-sdk/uva-s3"
)

// main entry point
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

	filename := path.Base(cfg.KeyName)

	// see if the file already exists locally
	exists := false
	if _, err := os.Stat(filename); err == nil {
		exists = true
	}

	// see if a glacier restore is in progress
	if o.IsGlacier() == true && o.IsRestoring() == true {
		log.Printf("INFO: object in glacier, restore is IN PROGRESS; terminating normally")

		// and done
		os.Exit(0)
	}

	// see if a glacier restore is done
	if o.IsGlacier() == true && o.IsRestored() == true {
		log.Printf("INFO: object in glacier and has been restored")

		if exists == true && cfg.Overwrite == false {
			log.Printf("INFO: exists locally without overwrite option; terminating normally")
			// and done
			os.Exit(0)
		}

		err = s3Svc.GetToFile(o, filename)
		if err != nil {
			log.Fatalf("ERROR: %s", err.Error())
		}
		log.Printf("INFO: available as %s; terminating normally", filename)

		// and done
		os.Exit(0)
	}

	// item is not in glacier
	if o.IsGlacier() == false {
		log.Printf("INFO: object NOT in glacier (or is glacier IR)")

		if exists == true && cfg.Overwrite == false {
			log.Printf("INFO: exists locally without overwrite option; terminating normally")
			// and done
			os.Exit(0)
		}

		err = s3Svc.GetToFile(o, filename)
		if err != nil {
			log.Fatalf("ERROR: %s", err.Error())
		}
		log.Printf("INFO: available as %s; terminating normally", filename)

		// and done
		os.Exit(0)
	}

	// object is in glacier and can be restored
	if cfg.Restore == true {
		log.Printf("INFO: object in glacier")

		//err = s3Svc.RestoreObject(o, uva_s3.RESTORE_EXPEDITED, int64(cfg.RestoreDays))
		err = s3Svc.RestoreObject(o, uva_s3.RESTORE_STANDARD, int64(cfg.RestoreDays))
		if err != nil {
			log.Fatalf("ERROR: %s", err.Error())
		}
		log.Printf("INFO: initiated restore; terminating normally")

	} else {
		log.Printf("INFO: object in glacier, can be restored; terminating normally")
	}

	// and done
	os.Exit(0)
}

//
// end of file
//
