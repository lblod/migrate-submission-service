# migrate-submission-service
Service responsible for migrating `toezicht:InzendingVoorToezicht` to `meb:Submssion` and its relations.\
This service is technically meant to run only once.

# API
All post calls
```
POST /delta
```
The following config should be added:
```
  {
    match: {
      predicate: {
        type: 'uri',
        value: 'http://www.w3.org/ns/adms#status'
      },
      object: {
        type: 'uri',
        value: 'http://data.lblod.info/document-statuses/verstuurd'
      }
    },
    callback: {
      url: 'http://migrate-submission/delta',
      method: 'POST'
    },
    options: {
      resourceFormat: 'v0.0.1',
      gracePeriod: 1000,
      ignoreFromSelf: true
    }
  },
  {
    match: {
      predicate: {
        type: 'uri',
        value: 'http://mu.semte.ch/vocabularies/ext/fileAddressCacheStatusLabel'
      }
    },
    callback: {
      url: 'http://migrate-submission/delta',
      method: 'POST'
    },
    options: {
      resourceFormat: 'v0.0.1',
      gracePeriod: 1000,
      ignoreFromSelf: true
    }
```


```
POST /start-migration-unprocessed
```
Runs the migrations where no migration task is linked to, takes the all known (and hardcoded) form versions in to account.

```
POST /start-migration-with-filter

Content-Type: application/json

Body

{
 "formNodeUri": "http://data.lblod.info/form-nodes/77fa3d4b1310b08f49ca334ac13153a5953a9feba2c6bfb7c555dc9d45a1d1d7",
 "bestuurseenheid": "http://data.lblod.info/id/bestuurseenheden/974816591f269bb7d74aa1720922651529f3d3b2a787f5c60b73e5a0384950a4",
 "inzendingUri": "http://uri/of/a/specific/toezichtInzendingVoorToezicht",
 "besluitType": "http://uri/of/a/specific/type/of/inzending/the/old/uris/check/besluit-types-list/file",
 "taskStatus": "",
 "inzendingStatus": "",
 "unprocessedMigrationsOnly": true
 "limit": 1
}
```
Runs the migration for specific query parameters.
- `formNodeUri`: options
```
'http://data.lblod.info/form-nodes/0ecb1654df3d058cf6a636237179e038a8dd65f4edaa3efdfd4d3b7f8311d354'  //2018
'http://data.lblod.info/form-nodes/3aa9e6897f9048d67af54837127db5bafb58aaa689bab1842510f0b17e6b1c05' //2019
'http://data.lblod.info/form-nodes/77fa3d4b1310b08f49ca334ac13153a5953a9feba2c6bfb7c555dc9d45a1d1d7' //latest
```
- `taskStatus`: if unspecified, it will fetch all inzendingen with no task associated to it. Else possible values are
```
'http://lblod.data.gift/concepts/migrate-submission-service/status/ongoing'
'http://lblod.data.gift/concepts/migrate-submission-service/status/finished'
'http://lblod.data.gift/concepts/migrate-submission-service/status/failed'
```
- `inzendingStatus`: options
```
'http://data.lblod.info/document-statuses/concept'
'http://data.lblod.info/document-statuses/verstuurd'
```
- `unprocessedMigrationsOnly`: Careful with this one. If set to false or undefined, you'll create duplicates possibly.


```
POST /remove-migration-with-filter

Content-Type: application/json

Body

{
 "formNodeUri": "http://data.lblod.info/form-nodes/77fa3d4b1310b08f49ca334ac13153a5953a9feba2c6bfb7c555dc9d45a1d1d7",
 "bestuurseenheid": "http://data.lblod.info/id/bestuurseenheden/974816591f269bb7d74aa1720922651529f3d3b2a787f5c60b73e5a0384950a4",
 "inzendingUri": "http://uri/of/a/specific/toezichtInzendingVoorToezicht",
 "besluitType": "http://uri/of/a/specific/type/of/inzending/the/old/uris/check/besluit-types-list/file",
 "taskStatus": "",
 "inzendingStatus": "",
 "unprocessedMigrationsOnly": true
 "limit": 1
}
```
Removes migration with filter. Same parameters apply as above.

```
POST /start-migration-debug
```
This is your playground where you can mess with code. There are som debug-helpers where you might want to play with.

# In docker-compose
```
  migrate-submission:
    image: lblod/migrate-submission-service:0.2.0
    environment:
      ACTIVE_FORM_FILE: "share://semantic-forms/20200508151443-forms.ttl"
    links:
      - virtuoso:database
    volumes:
      - ./data/files/submissions:/share/submissions
```
Don't forget to map port 80  to a port of your choice. And service will crash if no active form file is provided

# Caveats
- It is not thread safe, meaning it might do unexpected things when starting multiple jobs at the same time.
- This assumes the production setup of virtuoso for loket. Expects virtuoso to be able to return approx 60000 triples.
- It works on virtuoso, so will have to restart cache and resource.
