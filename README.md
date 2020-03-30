# migrate-submission-service
Service responsible for migrating `toezicht:InzendingVoorToezicht` to `meb:Submssion` and its relations.

# API
All post calls

```
POST /start-migration-all
```
Runs all migrations, takes the all known (and hardcoded) form versions in to account.

```
POST /start-migration-with-filter

Content-Type: application/json

Body

{
 "formNodeUri": "http://data.lblod.info/form-nodes/77fa3d4b1310b08f49ca334ac13153a5953a9feba2c6bfb7c555dc9d45a1d1d7",
 "bestuurseenheid": "http://data.lblod.info/id/bestuurseenheden/974816591f269bb7d74aa1720922651529f3d3b2a787f5c60b73e5a0384950a4",
 "inzendingUri": "http://uri/of/a/specific/toezichtInzendingVoorToezicht"
 "besluitType": "http://uri/of/a/specific/type/of/inzending/the/old/uris/check/besluit-types-list/file"
 "limit": 1
}
```
Runs the migration for specific query paramters.
`formNodeUri`: is required.

```
POST /start-migration-debug
```
This is your playground where you can mess with code. There are som debug-helpers where you might want to play with.
