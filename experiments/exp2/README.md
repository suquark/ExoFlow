# Business Logic

https://github.com/eniac/Beldi/blob/master/benchmark/hotel/workload.lua

Travel reservation (Cf. Expedia): Users can create an account,
search for hotels and flights, sort them by price/distance/rate,
find recommendations, and reserve hotel rooms and flights.
The workflow consists of 10 SSFs, and includes a cross-SSF
transaction to ensure that when a user reserves a hotel and a
flight, the reservation goes through only if both SSFs succeed.
Note that we extend this app to support flight reservations, as
the original implementation [12] only supports hotels.

Note: we have to set the default region in aws credential. E.g.,

```
[default]
aws_access_key_id=...
aws_secret_access_key=...
region=us-east-1
```

Call `create_dynamodb_table.py` to create dynamodb table for workflow.
