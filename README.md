# GoGraph3

A  development of a simple graph database using AWS's Dynamodb as the datastore. The code base will also support Google's Spanner datatabase with some further development which was undertaken in GoGraph2. 

State data, for restartability during loads of bulk data, is stored in a MySQL instance in the case of AWS.

A useful development from GoGraph3 is an object-database-mapping product that supports Dynamodb, MySQL and Spanner. Eliminates all boilerplate code and makes development condsiderably faster and more flexible. Early days but maybe something useful will come out it.

MethodDB now in its own repo
