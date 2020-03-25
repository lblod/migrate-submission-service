import { app, errorHandler } from 'mu';
import  { getInzendingVoorToezichtToDo,
          createTaskForMigration,
          updateTask,
          constructInzendingContentTtl,
          insertData,
          ONGOING,
          FINISHED
        } from './queries';

import { createDataBuckets } from './data-bucket-helpers';
import { insertTtlFile } from './file-helpers';
import { calculateMetaSnapshot } from './copied-code/enrich-submission-service/submission-document';

app.get('/', (req, res) => {
  res.send('Hello from migrate-submission-service');
});

app.get('/start-migration', async (req, res) => {
  //GET: InzendingVoorToezicht to do  (specifc rootNode)
  //This is the latest <http://data.lblod.info/form-nodes/77fa3d4b1310b08f49ca334ac13153a5953a9feba2c6bfb7c555dc9d45a1d1d7>
  let formNode = 'http://data.lblod.info/form-nodes/77fa3d4b1310b08f49ca334ac13153a5953a9feba2c6bfb7c555dc9d45a1d1d7';
  const bestuurseenheid = 'http://data.lblod.info/id/bestuurseenheden/974816591f269bb7d74aa1720922651529f3d3b2a787f5c60b73e5a0384950a4'; //gem Aalst
  let inzendingen = await getInzendingVoorToezichtToDo(formNode, bestuurseenheid);

  console.log(`Found ${inzendingen.length} for formNode ${formNode}`);

  let count = 0;
  for(let inzending of inzendingen){
    try {
    const task = await createTaskForMigration(inzending.inzendingUri);
    const inzendingTTl = await constructInzendingContentTtl(inzending.inzendingUri);
    const data = await createDataBuckets(inzendingTTl);
    await insertTtlFile(data.fileGraph.value, data.formTtlFile.value, data.turtleFormTtlContent, data.nTriplesFileGraph);
    await insertData(inzending.graph, data.nTriplesDbGraph);
    await calculateMetaSnapshot(data.subissionDocument.value);
      await updateTask(task.taskUri, 1, FINISHED);
    }
    catch(e){
      debugger;
    }
    ++count;
    console.log(count);
  }
});
app.use(errorHandler);
