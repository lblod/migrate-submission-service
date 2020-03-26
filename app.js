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
import besluitTypes from './besluit-types-list';

app.get('/', (req, res) => {
  res.send('Hello from migrate-submission-service');
});

async function getOneInzendingPerType(){
  let formNode = 'http://data.lblod.info/form-nodes/77fa3d4b1310b08f49ca334ac13153a5953a9feba2c6bfb7c555dc9d45a1d1d7';
  const bestuurseenheid = null;
  const inzending = null;
  const limit = 1;
  let inzendingen =[];
  for(let besluitType of besluitTypes){
    const results = await getInzendingVoorToezichtToDo(formNode, bestuurseenheid, inzending, besluitType, limit);
    inzendingen = [...inzendingen, ...results];
  }
  console.log(`Found ${inzendingen.length} for formNode ${formNode}`);
  return inzendingen;
}

async function getInzendingen(){
  //GET: InzendingVoorToezicht to do  (specifc rootNode)
  //This is the latest <http://data.lblod.info/form-nodes/77fa3d4b1310b08f49ca334ac13153a5953a9feba2c6bfb7c555dc9d45a1d1d7>
  let formNode = 'http://data.lblod.info/form-nodes/77fa3d4b1310b08f49ca334ac13153a5953a9feba2c6bfb7c555dc9d45a1d1d7';
  const bestuurseenheid = null; 'http://data.lblod.info/id/bestuurseenheden/974816591f269bb7d74aa1720922651529f3d3b2a787f5c60b73e5a0384950a4'; //gem Aalst
  const inzending = null; 'http://data.lblod.info/inzendingen-voor-toezicht/5DF3942CA3ACB60008000420';
  const besluitType = null; 'http://data.lblod.info/DecisionType/5b3955cc006323233e711c482f3a6bf39a8d3eba6bbdb2c672bdfcf2b2985b03';
  const limit = null;
  const inzendingen = await getInzendingVoorToezichtToDo(formNode, bestuurseenheid, inzending, besluitType, limit);
  console.log(`Found ${inzendingen.length} for formNode ${formNode}`);
  return inzendingen;
}

async function getSpecificInzending(){
let formNode = 'http://data.lblod.info/form-nodes/77fa3d4b1310b08f49ca334ac13153a5953a9feba2c6bfb7c555dc9d45a1d1d7';
  const bestuurseenheid = null; 'http://data.lblod.info/id/bestuurseenheden/974816591f269bb7d74aa1720922651529f3d3b2a787f5c60b73e5a0384950a4'; //gem Aalst
  const inzending = 'http://data.lblod.info/inzendingen-voor-toezicht/5E67C1A2A3ACB600080003B7';
  const besluitType = null; 'http://data.lblod.info/DecisionType/5b3955cc006323233e711c482f3a6bf39a8d3eba6bbdb2c672bdfcf2b2985b03';
  const limit = null;
  const inzendingen = await getInzendingVoorToezichtToDo(formNode, bestuurseenheid, inzending, besluitType, limit);
  console.log(`Found ${inzendingen.length} for formNode ${formNode}`);
  return inzendingen;
}

app.get('/start-migration', async (req, res) => {

  const inzendingen = await getSpecificInzending();

  let count = 0;
  for(let inzending of inzendingen){
    try {
      const task = await createTaskForMigration(inzending.inzendingUri);
      const inzendingTTl = await constructInzendingContentTtl(inzending.inzendingUri);
      const data = await createDataBuckets(inzendingTTl);
      await insertTtlFile(data.fileGraph.value, data.formTtlFile.value, data.turtleFormTtlContent, data.nTriplesFileGraph);
      await insertData(inzending.graph, data.nTriplesDbGraph);
      await calculateMetaSnapshot(data.subissionDocument.value);

      console.log(`Inserted data for ${inzending.eenheidLabel}`);
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
