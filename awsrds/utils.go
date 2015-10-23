package awsrds

import (
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/pivotal-golang/lager"
)

func UserAccount(iamsvc *iam.IAM) (string, error) {
	getUserInput := &iam.GetUserInput{}
	getUserOutput, err := iamsvc.GetUser(getUserInput)
	if err != nil {
		return "", err
	}

	userAccount := strings.Split(*getUserOutput.User.Arn, ":")

	return userAccount[4], nil
}

func BuilRDSTags(tags map[string]string) []*rds.Tag {
	var rdsTags []*rds.Tag

	for key, value := range tags {
		rdsTags = append(rdsTags, &rds.Tag{Key: aws.String(key), Value: aws.String(value)})
	}

	return rdsTags
}

func AddTagsToResource(resourceARN string, tags []*rds.Tag, rdssvc *rds.RDS, logger lager.Logger) error {
	addTagsToResourceInput := &rds.AddTagsToResourceInput{
		ResourceName: aws.String(resourceARN),
		Tags:         tags,
	}

	logger.Debug("add-tags-to-resource", lager.Data{"input": addTagsToResourceInput})

	addTagsToResourceOutput, err := rdssvc.AddTagsToResource(addTagsToResourceInput)
	if err != nil {
		logger.Error("aws-rds-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return err
	}

	logger.Debug("add-tags-to-resource", lager.Data{"output": addTagsToResourceOutput})

	return nil
}
