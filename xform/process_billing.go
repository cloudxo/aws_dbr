package main

import (
    "github.com/linkedin/goavro"

    "os"
    "log"
    "net/url"
    "encoding/json"
    // "time"
    // "fmt"
    "bytes"
    "strconv"
    "io"
    "io/ioutil"
    "encoding/csv"
    "archive/zip"
    "strings"

	"github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/credentials"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
)

const DEFAULT_REGION = "us-east-1"

func getClient(config Config, bucket *string) (*s3.S3, error) {
    var creds *credentials.Credentials

    if config.AccessKey != "" {
        creds = credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, config.AccessToken)
    } else {
        creds = credentials.NewEnvCredentials()
    }

    // By default make sure a region is specified
    base := s3.New(session.New(&aws.Config{ Credentials: creds, Region: aws.String(DEFAULT_REGION) }))

    params := &s3.HeadBucketInput{ Bucket: bucket }

    // Check to see if bucket exists
    _, err := base.HeadBucket(params)
    if err != nil {
        return nil, err
    }

    if loc, err := base.GetBucketLocation(&s3.GetBucketLocationInput{Bucket: bucket}); err != nil {
        return nil, err
     } else if (loc.LocationConstraint != nil) {
         return s3.New(session.New(&base.Client.Config, &aws.Config{Region: loc.LocationConstraint})), nil
     }
     return base, nil
}

func Process(config Config, source string, dest string, partition bool) error {
    src, err := url.Parse(source)
    if err != nil {
        log.Fatalf("[FATAL] Invalid source URL %v", err)
    }

    dst, err := url.Parse(dest)
    if err != nil {
        log.Fatalf("[FATAL] Invalid dest URL %v", err)
    }

    if src.Scheme != "s3" && src.Scheme != "file" && src.Scheme != "" {
        log.Fatalf("[FATAL] Invalid source URL expected s3://...")
    }
    if len(src.Path) < 2 {
        log.Fatalf("[FATAL] Invalid source URL expected s3://BUCKET/PATH")
    }
    if dst.Scheme != "s3" && dst.Scheme != "file" && dst.Scheme != "" {
        log.Fatalf("[FATAL] Invalid dst URL expected s3://...")
    }
    if len(dst.Path) < 2 {
        log.Fatalf("[FATAL] Invalid dst URL expected s3://BUCKET/PATH")
    }

    if src.Scheme == "s3" {
        input := s3.GetObjectInput{
            Bucket: aws.String(src.Host),
            Key: aws.String(src.Path[1:]),
        }

        client, err := getClient(config, input.Bucket)
        if err != nil {
            log.Fatalf("[FATAL] Error getting bucket %v", err)
        }

        //
        //
        //
        resp, err := client.GetObject(&input)
        if err != nil {
            log.Printf("[ERROR] Unable to download bucket=%s key=%s", *input.Bucket, *input.Key)
            return nil
        }

        return readAndLoad(config, resp.Body, *resp.ContentLength, src, dst, partition)
    }
    if src.Scheme == "file" || src.Scheme == "" {
        reader, err := os.Open(src.Path)
        if err != nil {
            log.Fatalf("[FATAL] Unable to open input path=%s", src.Path)
        }
        defer reader.Close()

        info, _ := reader.Stat()

        return readAndLoad(config, reader, info.Size(), src, dst, partition)
    }
    return nil
}

func readAndLoad(config Config, reader io.ReadCloser, contentLength int64, source, dst *url.URL, _ bool) error {
    var w   *os.File

    if body, err := ioutil.ReadAll(reader); err == nil {
        if int64(len(body)) != contentLength {
            log.Printf("[ERROR] Sized doesn't match expected source=%s got=%d expect=%d",
                            source.String(), len(body), contentLength)
            return nil
        }

        if dst.Scheme == "s3" {
            w, err = ioutil.TempFile("", "avro-build")
            if err != nil {
                return err
            }
            defer os.Remove(w.Name())
        } else {
            w, err = os.Create(dst.Path + ".tmp")
            if err != nil {
                return err
            }
            // Make sure we clean up (always)
            defer w.Close()
            defer os.Remove(dst.Path + ".tmp")
        }

        // Unzip files right here ... They should only contain one entry
        if strings.HasSuffix(source.Path, ".zip") {
            reader, err := zip.NewReader(bytes.NewReader(body), contentLength)

            if err != nil {
                log.Printf("[INFO] Unable to open zip file source=%s", source.String())
                return err
            }

            for _, file := range reader.File {
                // FIXME: Check that this isn't a Directory - shouldn't be

                log.Printf("[INFO] Open zip component name=%s file source=%s", file.Name, source.String())
                fileReader, err := file.Open()
                if err != nil {
                    log.Printf("[ERROR] Unable zip component name=%s file source=%s", file.Name, source.String())
                    continue
                }
                defer fileReader.Close()

                if body, err := ioutil.ReadAll(fileReader); err == nil {
                    processTagsFile(body, source, w)
                }
            }
        } else {
            processTagsFile(body, source, w)
        }
    }

    if w != nil {
        path := w.Name()
        w.Close()

        if dst.Scheme == "s3" {
            client, err := getClient(config, &dst.Host)
            if err != nil {
                log.Fatalf("[FATAL] Error getting dest bucket %v", err)
            }

            s, err := os.Open(path)
            if err != nil {
                log.Fatalf("[FATAL] Error getting file to copy %v", err)
            }
            defer s.Close()

            params := &s3.PutObjectInput{
                Bucket: aws.String(dst.Host),
                Key: aws.String(dst.Path),
                Body: s,
            }
            if _, err := client.PutObject(params); err != nil {
                log.Printf("[ERROR] Failed to upload file to S3", err)
            }
        } else {
            os.Rename(dst.Path + ".tmp", dst.Path)
        }
    }

    return nil
}

//
//  List of the fields in the DBR Record
//
const (
    InvoiceID = 0
    PayerAccountId = 1
    inkedAccountId = 2
    RecordType = 3
    RecordId = 4
    ProductName = 5
    RateId = 6
    SubscriptionId = 7
    PricingPlanId = 8
    UsageType = 9
    Operation = 10
    AvailabilityZone = 11
    ReservedInstance = 12
    ItemDescription = 13
    UsageStartDate = 14
    UsageEndDate = 15
    UsageQuantity = 16
    BlendedRate = 17
    BlendedCost = 18
    UnBlendedRate = 19
    UnBlendedCost = 20
    ResourceId = 21
)

type avroField struct {
    Type    interface{}  `json:"type"`
    Name    string  `json:"name"`
    inCSV   bool    `json:"_"`
}

type avroSchema struct {
    Type    string  `json:"type"`
    Name    string  `json:"name"`
    Fields  []avroField    `json:"fields"`
}

func processTagsFile(body []byte, source *url.URL, w io.Writer) {
    log.Printf("[INFO] Processing with-tags source=%s", source.String())

    reader := csv.NewReader(bytes.NewReader(body))

    // the CSV header
    var header  []string

    header, err := reader.Read()
    if err == io.EOF {
        // No header...
        log.Printf("[INFO] Input file is empty")
        return
    }
    if err != nil {
        log.Printf("[ERROR] Error reading input %v", err)
    }

    // Avro record schem is JSON, so build a struct and encode
    //   These fields are in order in which they come from the CSV file
    optDouble := []string{"null", "double"}
    optLong := []string{"null", "long"}
    optString := []string{"null", "string"}

    types := map[string]interface{}{
        "PayerAccountId": "long",
        "LinkedAccountId": "long",
        "SubscriptionId": &optLong,
        "PricingPlanId": &optLong,
        "UsageQuantity": &optDouble,
        "BlendedRate": &optDouble,
        "BlendedCost": &optDouble,
        "UnBlendedRate": &optDouble,
        "UnBlendedCost": &optDouble,
    }

    fields := make([]avroField, 0)

    for _, hdr := range header {
        t, found := types[hdr]
        if !found {
            t = "string"
        }

        hdr = strings.Replace(hdr, ":", "_", -1)

        fields = append(fields, avroField{ Name: hdr, Type: t, inCSV: true })
    }

    // Fields that we generate
    fields = append(fields, avroField{ Name: "Service", Type: &optString, inCSV: false })
    fields = append(fields, avroField{ Name: "InstanceUsage", Type: &optString, inCSV: false })
    fields = append(fields, avroField{ Name: "InstanceType", Type: &optString, inCSV: false })
    fields = append(fields, avroField{ Name: "InstanceClass", Type: &optString, inCSV: false })
    fields = append(fields, avroField{ Name: "SubscriptionPrepay", Type: "boolean", inCSV: false })
    fields = append(fields, avroField{ Name: "SubscriptionCharge", Type: "boolean", inCSV: false })
    fields = append(fields, avroField{ Name: "StartDate", Type: "long", inCSV: false })

    schema := avroSchema{
        Type: "record",
        Name: "Billing Report",
        Fields: fields,
    }

    // Open the Avro Writer
    recordSchemaB, err := json.Marshal(schema)
    if err != nil {
        log.Fatal("[FATAL] Unable to encode schema")
    }
    recordSchema := string(recordSchemaB)

    fw, err := goavro.NewWriter(
                    goavro.BlockSize(13), // example; default is 10
                    // goavro.Compression(goavro.CompressionSnappy), // default is CompressionNull
                    goavro.Compression(goavro.CompressionDeflate), // default is CompressionNull
                    goavro.WriterSchema(recordSchema),
                    goavro.ToWriter(w))
    if err != nil {
        log.Fatal("cannot create Writer: ", err)
    }
    defer fw.Close()

    //
    for {
        record, err := reader.Read()

        if err == io.EOF {
            break
        }
        if err != nil {
            log.Printf("[ERROR] %v", err)
        }

        arec, err := goavro.NewRecord(goavro.RecordSchema(recordSchema))
        if err != nil {
            log.Fatalf("[FATAL] %v", err)
        }

        for idx, field := range schema.Fields {
            if field.inCSV {
                switch (types[field.Name]) {
                case &optLong:
                    if record[idx] != "" {
                        if i, err := strconv.ParseInt(record[idx], 10, 64); err == nil {
                            arec.Set(field.Name, i)
                        } else {
                            log.Printf("[ERROR] converting field=%s err=%v", field.Name, err)
                        }
                    }
                case &optDouble:
                    if record[idx] != "" {
                        if f, err := strconv.ParseFloat(record[idx], 64); err == nil {
                            arec.Set(field.Name, f)
                        } else {
                            log.Printf("[ERROR] converting field=%s err=%v", field.Name, err)
                        }
                    }
                case "long":
                    if record[idx] == "" {
                        arec.Set(field.Name, int64(0))
                    } else {
                        if i, err := strconv.ParseInt(record[idx], 10, 64); err == nil {
                            arec.Set(field.Name, i)
                        } else {
                            log.Printf("[ERROR] converting field=%s err=%v", field.Name, err)
                        }
                    }
                case "double":
                    if record[idx] == "" {
                        arec.Set(field.Name, float64(0.0))
                    } else {
                        if f, err := strconv.ParseFloat(record[idx], 64); err == nil {
                            arec.Set(field.Name, f)
                        } else {
                            log.Printf("[ERROR] converting field=%s err=%v", field.Name, err)
                        }
                    }
                // case "string":
                default:
                    arec.Set(field.Name, record[idx])
                }
            }
        }

        arec.Set("Service", createServiceName(record))

        if record[Operation] == "RunInstances" {
            parts := strings.Split(record[UsageType], ":")

            if len(parts) == 2 {
                arec.Set("InstanceUsage", parts[0])
                arec.Set("InstanceType", parts[1])
                arec.Set("InstanceClass", strings.Split(parts[1], ".")[0])
            }
        }
        if record[ProductName] == "Amazon Elastic Compute Cloud" {
            arec.Set("SubscriptionPrepay", record[Operation] == "RunInstances" && record[ResourceId] == "")
            arec.Set("SubscriptionCharge", record[RateId] == "0")
        } else {
            arec.Set("SubscriptionPrepay", false)
            arec.Set("SubscriptionCharge", false)
        }

        sdate, _ := strconv.ParseInt(strings.Replace(strings.Split(record[UsageStartDate], " ")[0], "-", "", -1), 10, 64)
        arec.Set("StartDate", sdate)

        fw.Write(arec)
    }
}

var (
    nameTable = map[string]string{
        "Amazon RDS Service" : "RDS",
        "Amazon Elastic MapReduce" : "EMR",
        "Amazon Simple Storage Service" : "S3",
        "Amazon Simple Email Service" : "SES",
    }

    awsDataTransferOps = map[string]bool{
        "VPCPeering-In": true,
        "VPCPeering-Out": true,
        "PublicIP-In": true,
        "PublicIP-Out": true,
        "InterZone-In": true,
        "InterZone-Out": true,
        "NatGateway": true,
    }
)

func createServiceName(record []string) string {
    productName := record[ProductName]
    operation := record[Operation]
    usageType := record[UsageType]

    if productName == "" {
        return "Unknown"
    } else if productName == "Amazon Elastic Compute Cloud" {
        if operation == "" {
            return "EC2-Other"
        } else if strings.HasPrefix(usageType, "SpotUsage:") {
            return "EC2-Spot"
        } else if strings.HasPrefix(operation, "RunInstances") {
            return "EC2-Instances"
        } else if strings.HasPrefix(operation, "LoadBalancing") {
            return "EC2-ELB"
        } else if strings.Index(operation, "EBS:") != -1 || strings.Index(usageType, "EBS:") != -1 {
            return "EC2-EBS"
        } else if awsDataTransferOps[operation] {
            return "Data Transfer"
        } else {
            return "EC2-Other"
        }
    } else if productName == "Amazon RDS Service" {
        if usageType == "RDS:ChargedBackupUsage" {
            return "RDS:Backup"
        } else if strings.HasPrefix(usageType, "Aurora:") {
            return "RDS:Aurora"
        } else {
            return "RDS"
        }
    } else if name, found := nameTable[productName]; found {
        return name
    } else {
        if strings.HasPrefix(productName, "Amazon") {
            return strings.TrimSpace(productName[6:])
        } else if strings.HasPrefix(productName, "AWS") {
            return strings.TrimSpace(productName[3:])
        } else if productName == "" {
            return "Unknown"
        }
        return productName
    }
}
