package awsrds

import (
	"errors"
	"strings"

	"code.cloudfoundry.org/lager"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/rds"
)

// UserAccount Get IAM AWS user
func UserAccount(iamsvc *iam.IAM) (string, error) {
	getUserInput := &iam.GetUserInput{}
	getUserOutput, err := iamsvc.GetUser(getUserInput)
	if err != nil {
		return "", err
	}

	userAccount := strings.Split(*getUserOutput.User.Arn, ":")

	return userAccount[4], nil
}

// BuilRDSTags for RDS Objects
func BuilRDSTags(tags map[string]string) []*rds.Tag {
	var rdsTags []*rds.Tag

	for key, value := range tags {
		rdsTags = append(rdsTags, &rds.Tag{Key: aws.String(key), Value: aws.String(value)})
	}

	return rdsTags
}

// GetTags is getting tags based on Arn
func GetTags(arn *string, rdssvc *rds.RDS) (map[string]string, error) {
	tags := make(map[string]string)

	i := &rds.ListTagsForResourceInput{ResourceName: arn}
	tagOutput, err := rdssvc.ListTagsForResource(i)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			return tags, errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return tags, err
	}
	for _, t := range tagOutput.TagList {
		tags[aws.StringValue(t.Key)] = aws.StringValue(t.Value)
	}
	return tags, nil
}

// AddTagsToResource  for RDS Objects
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
