import bodyParser from 'body-parser';
import { app, errorHandler } from 'mu';
import  { getInzendingVoorToezicht,
          createTaskForMigration,
          updateTask,
          constructInzendingContentTtl,
          getTask,
          insertData,
          ONGOING,
          FAILED,
          FINISHED
        } from './queries';

import { createDataBuckets } from './data-bucket-helpers';
import { insertTtlFile } from './file-helpers';
import { calculateMetaSnapshot } from './copied-code/enrich-submission-service/submission-document';
import { getOneInzendingPerType } from './debug-helpers';

app.use(bodyParser.json({ type: function(req) { return /^application\/json/.test(req.get('content-type')); } }));

app.get('/', (req, res) => {
  res.send('Hello from migrate-submission-service');
});


app.post('/start-migration-all', async (req, res) => {
  const formNodeUris = [
    'http://data.lblod.info/form-nodes/0ecb1654df3d058cf6a636237179e038a8dd65f4edaa3efdfd4d3b7f8311d354',
    'http://data.lblod.info/form-nodes/3aa9e6897f9048d67af54837127db5bafb58aaa689bab1842510f0b17e6b1c05',
    'http://data.lblod.info/form-nodes/77fa3d4b1310b08f49ca334ac13153a5953a9feba2c6bfb7c555dc9d45a1d1d7'
  ];

  let inzendingen = [];
  for(let formNodeUri of formNodeUris){
    inzendingen = [...inzendingen, ...(await getInzendingVoorToezicht(formNodeUri))];
  }

  migrateFormsForFormNode(inzendingen);
  res.send({msg: `job started for ${inzendingen.length} inzendingen` });
});

app.post('/start-migration-with-filter', async (req, res) => {
  const { formNodeUri, bestuurseenheid, inzendingUri, besluitType, taskStatus, limit } = req.body;

  if(!formNodeUri){
    res.status(400).send({msg: `Please specify at least formNodeUri` });
    return;
  }

  const inzendingen = await getInzendingVoorToezicht(formNodeUri, bestuurseenheid, inzendingUri, besluitType, taskStatus, limit);
  migrateFormsForFormNode(inzendingen);
  res.send({msg: `job started for ${inzendingen.length} inzendingen` });
});

app.get('/start-migration-debug', async (req, res) => {
  console.log(`Here you can use debug-helpers and modify code to test stuff`);
  const inzendingen = await getOneInzendingPerType(); //you can also use other premade functions
  migrateFormsForFormNode(inzendingen);
  res.send({msg: 'debug job started' });
});

app.use(errorHandler);

async function migrateFormsForFormNode(inzendingen){
  const start = new Date();
  let count = 0;
  let errorCount = 0;
  const total = inzendingen.length;
  for(let inzending of inzendingen){
    let task = null;
    if(!inzending.task){
      task = await createTaskForMigration(inzending.inzendingUri);
    }
    else{
      task = await getTask(inzending.task);
      const retries = task.numberOfRetries ? parseInt(task.numberOfRetries) : 0;
      await updateTask(task.taskUri, retries + 1, ONGOING);
    }
    try {
      const inzendingTTl = await constructInzendingContentTtl(inzending.inzendingUri);
      const data = await createDataBuckets(inzendingTTl);
      await insertTtlFile(data.fileGraph.value, data.formTtlFile.value, data.turtleFormTtlContent, data.nTriplesFileGraph);
      await insertData(inzending.graph, data.nTriplesDbGraph);
      await calculateMetaSnapshot(data.subissionDocument.value);

      console.log(`Inserted data for ${inzending.eenheidLabel}`);
      await updateTask(task.taskUri, 1, FINISHED);
    }
    catch(e){
      console.log(`Error for ${inzending.inzendingUri}`);
      console.error(e);
      ++errorCount;
      await updateTask(task.taskUri, 1, FAILED);
    }
    ++count;
    console.log(`---------------- Processed ${count} of ${total} ----------------`);
  }

  const end = new Date();

  console.log(`Finished processing ${total} forms`);
  console.log(`Started at ${start}`);
  console.log(`Ended at ${end}`);
  console.log(`Total errors ${errorCount}`);
}
