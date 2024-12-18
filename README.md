# sysdb
In memory Key Value store like INI file over BoltDB. 

The basic idea is to use the reflection feature
of golang to store the key values in bolt db and still have the ability to serialize and deserialize
using strict type checking of golang.

The use cases are simple inmemory databases for configurations and events for any application, without elaborate
SQL kind of client server application.

The API also allows to have event callbacks on mutable operatios like Save, Delete ...



