package main

import (
	"context"
	"flag"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/cenkalti/backoff"
	"log"
	"os"
	"sync"
)

var (
	WarningLogger *log.Logger
	InfoLogger    *log.Logger
	ErrorLogger   *log.Logger
	verbosity     *bool
)

func main() {
	var bucketName = flag.String("b", "unknown", "Bucket name")
	verbosity = flag.Bool("v", false, "Set to verbose logging")
	flag.Parse()

	InfoLogger = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime)
	WarningLogger = log.New(os.Stdout, "WARN: ", log.Ldate|log.Ltime)
	ErrorLogger = log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime)

	if *bucketName == "unknown" {
		exitErrorf("You must specify a bucket name with -b")
	}
	bucketRegion := getRegion(*bucketName)
	if bucketRegion == "unknown" {
		exitErrorf("Unable to find bucket for %s\n", *bucketName)
	}
	InfoLogger.Printf("Bucket %s was found in %s\n", *bucketName, bucketRegion)

	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region: aws.String(bucketRegion),
		},
		SharedConfigState: session.SharedConfigEnable,
	})
	svc := s3.New(sess)

	if err != nil {
		exitErrorf("Unable to setup s3 connection: %v", err)
	}

	deleteAllVersions(*bucketName, bucketRegion, svc)
	deleteBucket(*bucketName, bucketRegion, svc)
}

func getRegion(bucketName string) string {
	sess := session.Must(session.NewSession())
	ctx := context.Background()
	region, err := s3manager.GetBucketRegion(ctx, sess, bucketName, "us-west-2")
	if err != nil {
		return "unknown"
	}
	return region
}

func deleteWorker(jobs <-chan s3.DeleteObjectInput, wg *sync.WaitGroup, svc *s3.S3, deleteType string) {
	defer wg.Done()
	for s3Object := range jobs {
		//backoff.Retry(func() error{
		//	_, err := svc.DeleteObject(&s3Object)
		//	if *verbosity {
		//		InfoLogger.Printf("Deleting %s: %s\n", *s3Object.Key, *s3Object.VersionId)
		//	}
		//	if err != nil {
		//		WarningLogger.Printf("Unable to delete %s %s: %s\n", deleteType, *s3Object.Key, *s3Object.VersionId)
		//		return err
		//	} else {
		//		return nil
		//	}
		//
		//}, backoff.NewExponentialBackOff())
		for i := 0; i < 4;i++ {
			_, err := svc.DeleteObject(&s3Object)
			if *verbosity {
				InfoLogger.Printf("Deleting %s: %s\n", *s3Object.Key, *s3Object.VersionId)
			}
			if err != nil {
				WarningLogger.Printf("RT: %d Unable to delete %s %s: %s\n", i, deleteType, *s3Object.Key, *s3Object.VersionId)
			} else {
				continue
			}

		}

	}

}

func deleteS3Object(s3Object s3.DeleteObjectInput, wg *sync.WaitGroup, svc *s3.S3, deleteType string) {
	defer wg.Done()
	attempt := 1
	
	backoff.Retry(func() error{
		_, err := svc.DeleteObject(&s3Object)
		if *verbosity {
			InfoLogger.Printf("Deleting %s: %s\n", *s3Object.Key, *s3Object.VersionId)
		}
		if err != nil {
			WarningLogger.Printf("RT: %d Unable to delete %s %s: %s\n", attempt, deleteType, *s3Object.Key, *s3Object.VersionId)
			return err
		} else {
			return nil
		}

	}, backoff.NewExponentialBackOff())
	//_, err := svc.DeleteObject(&s3Object)
	//if *verbosity {
	//	InfoLogger.Printf("Deleting %s: %s\n", *s3Object.Key, *s3Object.VersionId)
	//}
	//if err != nil {
	//	WarningLogger.Printf("Unable to delete %s %s: %s\n", deleteType, *s3Object.Key, *s3Object.VersionId)
	//}
}


//TODO: See if there is a way to make this generic to fit the two types (for three would be a bonus)
func deleteMarkers(deleteMarkers []*s3.DeleteMarkerEntry, svc *s3.S3, bucketName string) *sync.WaitGroup {
	var wg sync.WaitGroup
	InfoLogger.Print("Deleting Delete Markers...")
	for _, deleteMarker := range deleteMarkers {
		wg.Add(1)
		go deleteS3Object(s3.DeleteObjectInput{
							Key:       deleteMarker.Key,
							VersionId: deleteMarker.VersionId,
							Bucket:    &bucketName,
						},
						&wg,
						svc,
						"Marker",
		)
	}
	return &wg
}

func deleteVersions(deleteVersions []*s3.ObjectVersion, svc *s3.S3, bucketName string) *sync.WaitGroup {
	var wg sync.WaitGroup
	InfoLogger.Print("Deleting Versions...")
	for _, version := range deleteVersions {
		wg.Add(1)
		go deleteS3Object(s3.DeleteObjectInput{
							Key:       version.Key,
							VersionId: version.VersionId,
							Bucket:    &bucketName,
						},
						&wg,
						svc,
						"Version",
		)
	}
	return &wg
}

func deleteObjects(deleteObjectsList []*s3.Object, svc *s3.S3, bucketName string) *sync.WaitGroup {
	var wg sync.WaitGroup
	InfoLogger.Print("Deleting Versions...")
	for _, content := range deleteObjectsList {
		wg.Add(1)
		go deleteS3Object(s3.DeleteObjectInput{
			Key:       content.Key,
			Bucket:    &bucketName,
		},
			&wg,
			svc,
			"Object",
		)
	}
	return &wg
}

func deleteAllVersions(bucketName string, region string, svc *s3.S3) bool {
	//Go through all pages of Object Versions and delete them
	err := svc.ListObjectVersionsPages(&s3.ListObjectVersionsInput{Bucket: aws.String(bucketName)},
		func(page *s3.ListObjectVersionsOutput, lastPage bool) bool {
			deleteMarkers(page.DeleteMarkers, svc, bucketName).Wait()
			//Although there is no race condition in the code, there is on the server side
			//10 milliseconds seems to be enough of a wait time between deletion of a marker
			//and it's version
			//time.Sleep(10 * time.Millisecond)
			deleteVersions(page.Versions, svc, bucketName).Wait()
			return !lastPage
		})
	if err != nil {
		exitErrorf("Unable to do versioning things for %q, %v", bucketName, err)
	}

	InfoLogger.Print("Deleting all Objects...")
	//Go through all pages of Objects and delete them
	//TODO: Move the inner function outside like we did above
	err = svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{Bucket: aws.String(bucketName)},
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			deleteObjects(page.Contents, svc, bucketName).Wait()
			return true
		})
	return true
}

func deleteBucket(bucketName string, region string, svc *s3.S3) bool {
	if *verbosity {
		InfoLogger.Printf("Deleting bucket %s....", bucketName)
	}

	_, err := svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		exitErrorf("Unable to delete bucket %s", bucketName)
	}
	InfoLogger.Printf("Deleted bucket %s", bucketName)
	return true
}

func exitErrorf(msg string, args ...interface{}) {
	ErrorLogger.Printf(msg+"\n", args...)
	os.Exit(1)
}
