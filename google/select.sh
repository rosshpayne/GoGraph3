gcloud spanner databases execute-sql $1 --instance=test-instance  --sql='select * from Cities where CountryId = 44'
