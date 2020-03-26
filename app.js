import { app, errorHandler } from 'mu';
import  { getInzendingVoorToezichtToDo,
          createTaskForMigration,
          updateTask,
          constructInzendingContentTtl,
          insertData,
          ONGOING,
          FAILED,
          FINISHED
        } from './queries';

import { createDataBuckets } from './data-bucket-helpers';
import { insertTtlFile } from './file-helpers';
import { calculateMetaSnapshot } from './copied-code/enrich-submission-service/submission-document';

async function migrateFormsForFormNode(formNode){
  const start = new Date();
  const inzendingen = await getInzendingVoorToezichtToDo(formNode);

  let count = 0;
  let errorCount = 0;
  const total = inzendingen.length;
  for(let inzending of inzendingen){
    const task = await createTaskForMigration(inzending.inzendingUri);
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

app.get('/', (req, res) => {
  res.send('Hello from migrate-submission-service');
});

app.get('/start-migration', async (req, res) => {
  let formNode = 'http://data.lblod.info/form-nodes/77fa3d4b1310b08f49ca334ac13153a5953a9feba2c6bfb7c555dc9d45a1d1d7';
  migrateFormsForFormNode(formNode);
  res.send({msg: 'job started' });
});

app.use(errorHandler);
