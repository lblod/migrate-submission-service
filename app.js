import bodyParser from 'body-parser';
import flatten from 'lodash.flatten';
import { app, errorHandler } from 'mu';
import  { getInzendingVoorToezicht } from './queries';
import { getOneInzendingPerType } from './debug-helpers';
import { migrateInzendingen, removeMigratedData } from './pipelines';

app.use(bodyParser.json({ type: function(req) { return /^application\/json/.test(req.get('content-type')); } }));

app.get('/', (req, res) => {
  res.send('Hello from migrate-submission-service');
});

app.post('/delta', async function(req, res, next) {
  const subjects = getSubjectsToProcess(req.body);
  if (!subjects.length) {
    console.log("Delta does not contain subjects to process");
    return res.status(204).send();
  }
  subjects.forEach(async subject => {
    const inzendingen = await getInzendingVoorToezicht(undefined,
                                                       undefined,
                                                       subject,
                                                       undefined,
                                                       undefined,
                                                       undefined,
                                                       undefined,
                                                       true);
    if(!inzendingen.length == 1){
      console.warn(`Found ${inzendingen.length} for ${subject} expected 1. Skipping`);
    }
    else {
      migrateInzendingen(inzendingen);
    }
  });
  return res.status(200).send({ data: subjects });
});


app.post('/start-migration-unprocessed', async (req, res) => {
  const formNodeUris = [
    'http://data.lblod.info/form-nodes/0ecb1654df3d058cf6a636237179e038a8dd65f4edaa3efdfd4d3b7f8311d354',
    'http://data.lblod.info/form-nodes/3aa9e6897f9048d67af54837127db5bafb58aaa689bab1842510f0b17e6b1c05',
    'http://data.lblod.info/form-nodes/77fa3d4b1310b08f49ca334ac13153a5953a9feba2c6bfb7c555dc9d45a1d1d7'
  ];

  let inzendingen = [];
  for(let formNodeUri of formNodeUris){
    const inzendingenToAdd = (await getInzendingVoorToezicht(formNodeUri,
                                                             undefined,
                                                             undefined,
                                                             undefined,
                                                             undefined,
                                                             undefined,
                                                             undefined,
                                                             true));
    inzendingen = [...inzendingen, ...inzendingenToAdd];
  }

  migrateInzendingen(inzendingen);
  res.send({msg: `job started for ${inzendingen.length} inzendingen` });
});

app.post('/start-migration-with-filter', async (req, res) => {
  const { formNodeUri, bestuurseenheid, inzendingUri, besluitType, taskStatus, limit, inzendingStatus, unprocessedMigrationsOnly } = req.body;
  const inzendingen = await getInzendingVoorToezicht(formNodeUri,
                                                     bestuurseenheid,
                                                     inzendingUri,
                                                     besluitType,
                                                     taskStatus,
                                                     limit,
                                                     inzendingStatus,
                                                     unprocessedMigrationsOnly);
  migrateInzendingen(inzendingen);
  res.send({msg: `job started for ${inzendingen.length} inzendingen` });
});

app.post('/remove-migration-with-filter', async (req, res) => {
  const { formNodeUri, bestuurseenheid, inzendingUri, besluitType, taskStatus, limit, inzendingStatus, unprocessedMigrationsOnly } = req.body;
  const inzendingen = await getInzendingVoorToezicht(formNodeUri,
                                                     bestuurseenheid,
                                                     inzendingUri,
                                                     besluitType,
                                                     taskStatus,
                                                     limit,
                                                     inzendingStatus,
                                                     unprocessedMigrationsOnly);
  removeMigratedData(inzendingen);
  res.send({msg: `job started for ${inzendingen.length} inzendingen` });
});

app.post('/start-migration-debug', async (req, res) => {
  console.log(`Here you can use debug-helpers and modify code to test stuff`);
  const inzendingen = await getOneInzendingPerType(); //you can also use other premade functions
  migrateInzendingen(inzendingen);
  res.send({msg: `debug job started for ${inzendingen.length} inzendingen` });
});

app.use(errorHandler);

function getSubjectsToProcess(delta) {
  const inserts = flatten(delta.map(changeSet => changeSet.inserts));
  return inserts.filter(isTriggerTriple).map(t => t.subject.value);
}

function isTriggerTriple(triple) {
  return triple.predicate.value == 'http://www.w3.org/ns/adms#status'
    && triple.object.value == "http://data.lblod.info/document-statuses/verstuurd";
};
