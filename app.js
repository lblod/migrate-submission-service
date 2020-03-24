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
  for(let inzending of inzendingen.slice(1, 10)){
    const task = await createTaskForMigration(inzending.inzendingUri);
    const inzendingTTl = await constructInzendingContentTtl(inzending.inzendingUri);
    const data = createDataBuckets(inzendingTTl);
    await insertTtlFile(data.fileGraph.value, data.formTtlFile.value, data.turtleFormTtlContent, data.nTriplesFileGraph);
    await insertData(inzending.graph, data.nTriplesDbGraph);
    ++count;
    console.log(count);
  }
  //CREATE: task per Inzending

  //create the TTL

  //create the submission

  //create the melding:FormData


});
// /*
//  * DELTA HANDLING
//  */

// app.post('/delta', async function(req, res, next) {
//   const tasks = getAutomaticSubmissionTasks(req.body);
//   if (!tasks.length) {
//     console.log("Delta does not contain an automatic submission task with status 'ready-for-validation'. Nothing should happen.");
//     return res.status(204).send();
//   }

//   for (let task of tasks) {
//     try {
//       await updateTaskStatus(task, TASK_ONGOING_STATUS);
//       const submission = await getSubmissionByTask(task);

//       const handleAutomaticSubmission = async () => {
//         try {
//           const resultingStatus = await submission.process();
//           if(resultingStatus == SENT_STATUS){
//             await updateTaskStatus(task, TASK_SUCCESSFUL_SENT_STATUS);
//           }
//           else{
//             await updateTaskStatus(task, TASK_SUCCESSFUL_CONCEPT_STATUS);
//           }
//         } catch (e) {
//           await updateTaskStatus(task, TASK_FAILURE_STATUS);
//         }
//       };

//       handleAutomaticSubmission(); // async processing
//     } catch (e) {
//       console.log(`Something went wrong while handling deltas for automatic submission task ${task}`);
//       console.log(e);
//       try {
//         await updateTaskStatus(task, TASK_FAILURE_STATUS);
//       } catch (e) {
//         console.log(`Failed to update state of task ${task} to failure state. Is the connection to the database broken?`);
//       }
//       return next(e);
//     }
//   }

//   return res.status(200).send({ data: tasks });
// });

// /**
//  * Returns the automatic submission tasks that are ready for validation
//  * from the delta message. An empty array if there are none.
//  *
//  * @param Object delta Message as received from the delta notifier
// */
// function getAutomaticSubmissionTasks(delta) {
//   const inserts = flatten(delta.map(changeSet => changeSet.inserts));
//   return inserts.filter(isTriggerTriple).map(t => t.subject.value);
// }

// /**
//  * Returns whether the passed triple is a trigger for the validation process
//  *
//  * @param Object triple Triple as received from the delta notifier
// */
// function isTriggerTriple(triple) {
//   return triple.predicate.value == 'http://www.w3.org/ns/adms#status'
//     && triple.object.value == TASK_READY_FOR_VALIDATION_STATUS;
// };


// /*
//  * SUBMISSION FORM ENDPOINTS
//  */

// /**
//  * Update the additions and deletions of a submission form. The source, meta and form cannot be updated.
// */
// app.put('/submission-documents/:uuid', async function(req, res, next) {
//   const uuid = req.params.uuid;
//   const submission = await getSubmissionBySubmissionDocument(uuid);

//   if (submission) {
//     try {
//       if (submission.status == SENT_STATUS) {
//         return res.status(422).send({ title: `Submission ${submission.uri} already submitted` });
//       } else {
//         const { additions, removals } = req.body;
//         await submission.update({ additions, removals });
//         return res.status(204).send();
//       }
//     } catch (e) {
//       console.log(`Something went wrong while updating submission with id ${uuid}`);
//       console.log(e);
//       return next(e);
//     }
//   } else {
//     return res.status(404).send({ title: `Submission ${uuid} not found` });
//   }

// });

// /**
//  * Submit a submission document
//  * I.e. validate the filled in form. If it's valid, update the status of the submission to 'sent'
// */
// app.post('/submission-documents/:uuid/submit', async function(req, res, next) {
//   const uuid = req.params.uuid;
//   const submission = await getSubmissionBySubmissionDocument(uuid);

//   if (submission) {
//     try {
//       if (submission.status == SENT_STATUS) {
//         return res.status(422).send({ title: `Submission ${submission.uri} already submitted` });
//       } else {
//         await submission.updateStatus(SUBMITABLE_STATUS);
//         const newStatus = await submission.process();
//         if (newStatus == SENT_STATUS) {
//           return res.status(204).send();
//         } else {
//           return res.status(400).send({ title: 'Unable to submit form' });
//         }
//       }
//     }
//     catch (error){
//       await submission.updateStatus(CONCEPT_STATUS);
//       console.log(`Something went wrong while submitting submission with id ${uuid}`);
//       console.log(error);
//       return next(error);
//     }
//   } else {
//     return res.status(404).send({ title: `Submission ${uuid} not found` });
//   }
// });


app.use(errorHandler);
