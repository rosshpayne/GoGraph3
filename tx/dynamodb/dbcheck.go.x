// build+ dynamodb

package db

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws/awserr"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func CheckTableExist(tb []string) error {
	var (
		err     error
		errored bool
		aerr    awserr.Error
	)
	for _, v := range tb {

		input := &dynamodb.DescribeTableInput{
			TableName: aws.String(v),
		}
		_, err = dynSrv.DescribeTable(input)
		if err != nil {

			if errors.As(err, &aerr) {
				switch aerr.Code() {
				case dynamodb.ErrCodeResourceNotFoundException:
					// do something....
					fmt.Println(aerr.Error())
				case dynamodb.ErrCodeInternalServerError:
					// do something....
					fmt.Println(aerr.Error())
				default:
					fmt.Println(aerr.Error())
				}
			} else {
				fmt.Println(aerr.Error())
			}
			errored = true
		}
	}
	if errored {
		return err
	}
	return nil

}
