Take a AWS Detailed Billing Report and convert to AVRO format

This also adds the following fieles

  * StartDate -- YYYYMMDD version of the data as an int
  * SubscriptionCharge -- boolean if this is a subscription charge
  * SubscriptionPrepay -- boolean if this is the month start subscription prepay 
  * InstanceUsage -- Spot or Ondemand
  * InstanceClass -- c3, t2, etc.
  * InstanceType -- c3.xlarge
  * Service -- simplified service names that match (with some extras) the AWS Bill explorer

---

handler.py -- a function to plug into AWS Lambda to generate these

---

Building on a raw ec2 instance -- should be a t2.medium for larger DBR files

Install GO - from the downloads

Build

    GOROOT=/usr/local/go GOPATH=$HOME/go make
