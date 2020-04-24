import  { getInzendingVoorToezicht,
          createTaskForMigration,
          updateTask,
          constructInzendingContentTtl,
          getTasksByStatus,
          getTask,
          insertData,
          ONGOING,
          FAILED,
          FINISHED,
          SCHEDULED,
          getMigratedTtlFilesFromInzending,
          removeTtlFileMeta,
          deleteMigratedInzendingData,
          getFileAddressDataFromInzending,
          getInzendingenRelatedTofileAddressStatus
        } from './queries';

import flatten from 'lodash.flatten';
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
      await insertData(data.remoteDataObjectGraph.value, data.nTriplesRemoteDataObjectGraph);
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

async function processDataFromDelta(delta){

  const inzendingenUris = getInzendingenToProcess(delta);
  for(const inzendingUri of inzendingenUris){
    await processInzendingFromDeltaWhichIsComplete(inzendingUri);
  }
  const addressStatuses = getFileAddressDataToProcess(delta);

  for(const statusUri of addressStatuses){
    await processInzendingFromFileAddressStatus(statusUri);
  }
}

async function processInzendingFromFileAddressStatus(statusUri){
  const inzendingenDatas = await getInzendingenRelatedTofileAddressStatus(statusUri);
  for(const inzendingData of inzendingenDatas){
    const fileAddressDatas = await getFileAddressDataFromInzending(inzendingData.inzendingUri);

    if(!await areAddressesCached(fileAddressDatas)){
      console.log(`Addresses for ${inzendingData.inzendingUri} not ready yet, wait for next notification`);
    }

    else {
      // The inzending is complete.
      // The next call will fetch inzendingen where no job is assiociated with.
      // (This should cover the case where the delta information we receive is outdated.)
      const inzendingen = (await getInzendingVoorToezicht(undefined,
                                                           undefined,
                                                           inzendingData.inzendingUri,
                                                           undefined,
                                                           undefined,
                                                           undefined,
                                                           'http://data.lblod.info/document-statuses/verstuurd',
                                                           true));
      if(!inzendingen.length) return;
      await migrateInzendingen( inzendingen );
    }
  }
}

async function processInzendingFromDeltaWhichIsComplete(inzendingUri){
  const inzendingen = (await getInzendingVoorToezicht(undefined,
                                                       undefined,
                                                       inzendingUri,
                                                       undefined,
                                                       undefined,
                                                       undefined,
                                                       'http://data.lblod.info/document-statuses/verstuurd',
                                                       true));
  if(!inzendingen.length) return;

  const inzending = inzendingen[0];
   //check if there is some file address data there
  const fileAddressDatas = await getFileAddressDataFromInzending(inzending.inzendingUri);

  //If no file address was provided, we can move on. There is no caching of the file ongoing
  if(!fileAddressDatas.length){
    await migrateInzendingen([ inzending ]);
  }
  //All adresses are cached, go for it!
  else if(fileAddressDatas.length && await areAddressesCached(fileAddressDatas)){
    await migrateInzendingen([ inzending ]);
  }
  else {
    console.log('we need to wait for the cached files of fileAddress');
  }
}

function getInzendingenToProcess(delta) {
  const inserts = flatten(delta.map(changeSet => changeSet.inserts));
  return inserts.filter(isInzendingTriple).map(t => t.subject.value);
}

function isInzendingTriple(triple) {
  return triple.predicate.value == 'http://www.w3.org/ns/adms#status'
    && triple.object.value == "http://data.lblod.info/document-statuses/verstuurd";
}

function getFileAddressDataToProcess(delta){
  const inserts = flatten(delta.map(changeSet => changeSet.inserts));
  return inserts.filter(isFileAddressData).map(t => t.subject.value);
}

function isFileAddressData(triple){
  return triple.predicate.value == 'http://mu.semte.ch/vocabularies/ext/fileAddressCacheStatusLabel';
}

async function areAddressesCached(fileAddressDatas){
  for(const fileAddressData of fileAddressDatas){
    if(!fileAddressData.fileAddressStatus){
      return false;
    }
    if(fileAddressData.fileAddressStatusLabel == 'pending'){
      return false;
    }
  }
  return true;
}

export {
  migrateInzendingen,
  removeMigratedData,
  processDataFromDelta
}
