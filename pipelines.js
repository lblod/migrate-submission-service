import  {
          createTaskForMigration,
          updateTask,
          constructInzendingContentTtl,
          getTask,
          insertData,
          ONGOING,
          FAILED,
          FINISHED,
          getMigratedTtlFilesFromInzending,
          removeTtlFileMeta,
          deleteMigratedInzendingData
        } from './queries';

import { createDataBuckets } from './data-bucket-helpers';
import { insertTtlFile, removeFile } from './file-helpers';
import { calculateMetaSnapshot } from './copied-code/enrich-submission-service/submission-document';

async function migrateInzendingen(inzendingen){
  const start = new Date();
  let count = 0;
  let errors = [];
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
      const errorMessage = `Error for ${inzending.inzendingUri}: error ${e}`;
      console.log(errorMessage);
      errors.push(errorMessage);
      await updateTask(task.taskUri, 1, FAILED);
    }
    ++count;
    console.log(`---------------- Processed ${count} of ${total} ----------------`);
  }

  const end = new Date();

  console.log(`----------------- Start Report -----------------`);
  console.log(`Finished processing ${total} forms`);
  console.log(`Started at ${start}`);
  console.log(`Ended at ${end}`);
  console.log(`----------------- Number of Errors ${errors.length} -----------------`);
  errors.forEach(err => console.log(err));
  console.log(`----------------- End Report -----------------`);
}

async function removeMigratedData(inzendingen){
  const start = new Date();
  let count = 0;
  let errors = [];
  const total = inzendingen.length;
  for(let inzending of inzendingen){
    if(!inzending.task){
      console.warn(`No task found for ${inzending.inzendingUri}, skipping`);
      continue;
    }
    try {
      const fileDatas = await getMigratedTtlFilesFromInzending(inzending.inzendingUri);
      await deleteMigratedInzendingData(inzending.inzendingUri);
      for(const fileData of fileDatas) {
        await removeTtlFileMeta(fileData.fileUri);
        await removeFile(fileData.fileUri);
      }
      console.log(`Removed data for ${inzending.eenheidLabel}`);
    }
    catch(e){
      const errorMessage = `Error for ${inzending.inzendingUri}: error ${e}`;
      console.log(errorMessage);
      errors.push(errorMessage);
    }
    ++count;
    console.log(`---------------- Processed ${count} of ${total} ----------------`);
  }

  const end = new Date();

  console.log(`----------------- Start Report -----------------`);
  console.log(`Finished processing ${total} forms`);
  console.log(`Started at ${start}`);
  console.log(`Ended at ${end}`);
  console.log(`----------------- Number of Errors ${errors.length} -----------------`);
  errors.forEach(err => console.log(err));
  console.log(`----------------- End Report -----------------`);
}

export {
  migrateInzendingen,
  removeMigratedData
}
