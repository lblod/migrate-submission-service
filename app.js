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

app.get('/', (req, res) => {
  res.send('Hello from migrate-submission-service');
});

app.get('/start-migration', async (req, res) => {
  //GET: InzendingVoorToezicht to do  (specifc rootNode)
  //This is the latest <http://data.lblod.info/form-nodes/77fa3d4b1310b08f49ca334ac13153a5953a9feba2c6bfb7c555dc9d45a1d1d7>
  let formNode = 'http://data.lblod.info/form-nodes/77fa3d4b1310b08f49ca334ac13153a5953a9feba2c6bfb7c555dc9d45a1d1d7';
  let inzendingen = await getInzendingVoorToezichtToDo(formNode);

  console.log(`Found ${inzendingen.length} for formNode ${formNode}`);

  let count = 0;
  for(let inzending of inzendingen){
    const task = await createTaskForMigration(inzending.inzendingUri);
    const inzendingTTl = await constructInzendingContentTtl(inzending.inzendingUri);
    const data = createDataBuckets(inzendingTTl);
    await insertTtlFile(data.fileGraph.value, data.formTtlFile.value, data.turtleFormTtlContent, data.nTriplesFileGraph);
    await insertData(inzending.graph, data.nTriplesDbGraph);
    await updateTask(task.taskUri, 1, FINISHED);
    ++count;
    console.log(count);
  }
});
app.use(errorHandler);
