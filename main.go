package main

import (
	"context"
	"flag"
	"log"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
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

func deleteWorker(jobs <-chan s3.DeleteObjectInput, wg *sync.WaitGroup, svc *s3.S3) {
	defer wg.Done()
	for s3Object := range jobs {
		_, err := svc.DeleteObject(&s3Object)
		if *verbosity {
			InfoLogger.Printf("Deleting %s: %s\n", *s3Object.Key, *s3Object.VersionId)
		}
		if err != nil {
			WarningLogger.Printf("Unable to delete %s: %s\n", *s3Object.Key, *s3Object.VersionId)
		}
	}

}

func deleteAllVersions(bucketName string, region string, svc *s3.S3) bool {
	channelSize := 1000
	workerCount := 50

	markerJobs := make(chan s3.DeleteObjectInput, channelSize)
	versionJobs := make(chan s3.DeleteObjectInput, channelSize)
	bucketJobs := make(chan s3.DeleteObjectInput, channelSize)

	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go deleteWorker(markerJobs, &wg, svc)
	}

	//Go through all pages of Object Versions and delete them
	pageNum := 0
	err := svc.ListObjectVersionsPages(&s3.ListObjectVersionsInput{Bucket: aws.String(bucketName)},
		func(page *s3.ListObjectVersionsOutput, lastPage bool) bool {
			if *verbosity {
				InfoLogger.Printf("Page %d: %d versions\n", pageNum, len(page.Versions))
			}
			InfoLogger.Print("Deleting Delete Markers...")
			for _, deleteMarker := range page.DeleteMarkers {
				key := deleteMarker.Key
				versionId := deleteMarker.VersionId
				markerJobs <- s3.DeleteObjectInput{
					Key:       key,
					VersionId: versionId,
					Bucket:    &bucketName,
				}
			}
			close(markerJobs)
			wg.Wait()

			versionJobs = make(chan s3.DeleteObjectInput, channelSize)
			for i := 0; i < workerCount; i++ {
				wg.Add(1)
				go deleteWorker(versionJobs, &wg, svc)
			}
			InfoLogger.Print("Deleting Versions...")
			for _, version := range page.Versions {
				key := version.Key
				versionId := version.VersionId

				versionJobs <- s3.DeleteObjectInput{
					Key:       key,
					VersionId: versionId,
					Bucket:    &bucketName,
				}
			}
			close(versionJobs)
			wg.Wait()

			return !lastPage
		})
	if err != nil {
		exitErrorf("Unable to do versioning things for %q, %v", bucketName, err)
	}

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go deleteWorker(bucketJobs, &wg, svc)
	}
	InfoLogger.Print("Deleting all Objects...")
	//Go through all pages of Objects and delete them
	err = svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{Bucket: aws.String(bucketName)},
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, content := range page.Contents {
				key := content.Key
				//Do the delete here
				bucketJobs <- s3.DeleteObjectInput{
					Key:    key,
					Bucket: &bucketName,
				}
			}
			close(bucketJobs)
			wg.Wait()
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
