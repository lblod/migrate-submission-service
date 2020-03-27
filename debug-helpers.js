import besluitTypes from './besluit-types-list';
import  { getInzendingVoorToezichtToDo } from './queries';
/**
 * Since this migration is all about testing, a wrote some functions which might be helpful for debugging purposes.
 * You will however have to modify code in app.js to use them.
 * There are three forms currently
 *   FORMNODE                                                                                            COUNT
 *   http://data.lblod.info/form-nodes/0ecb1654df3d058cf6a636237179e038a8dd65f4edaa3efdfd4d3b7f8311d354	7927
 *   http://data.lblod.info/form-nodes/3aa9e6897f9048d67af54837127db5bafb58aaa689bab1842510f0b17e6b1c05	31283
 *   http://data.lblod.info/form-nodes/77fa3d4b1310b08f49ca334ac13153a5953a9feba2c6bfb7c555dc9d45a1d1d7	18013 -> this is the latest
 */

/***********************************************************
 * FILTER FUNCTIONS
 ***********************************************************/
async function getInzendingen(){
  //This is the latest <http://data.lblod.info/form-nodes/77fa3d4b1310b08f49ca334ac13153a5953a9feba2c6bfb7c555dc9d45a1d1d7>
  let formNode = 'http://data.lblod.info/form-nodes/77fa3d4b1310b08f49ca334ac13153a5953a9feba2c6bfb7c555dc9d45a1d1d7';
  const bestuurseenheid = null; // 'http://data.lblod.info/id/bestuurseenheden/974816591f269bb7d74aa1720922651529f3d3b2a787f5c60b73e5a0384950a4'; //gem Aalst
  const inzending = null;  // 'http://data.lblod.info/inzendingen-voor-toezicht/5DF3942CA3ACB60008000420';
  const besluitType = null; // x'http://data.lblod.info/DecisionType/5b3955cc006323233e711c482f3a6bf39a8d3eba6bbdb2c672bdfcf2b2985b03';
  const limit = null;
  const inzendingen = await getInzendingVoorToezichtToDo(formNode, bestuurseenheid, inzending, besluitType, limit);
  console.log(`Found ${inzendingen.length} for formNode ${formNode}`);
  return inzendingen;
}

async function getSpecificInzending(){
  let formNode = 'http://data.lblod.info/form-nodes/77fa3d4b1310b08f49ca334ac13153a5953a9feba2c6bfb7c555dc9d45a1d1d7';
  const bestuurseenheid = null; // 'http://data.lblod.info/id/bestuurseenheden/974816591f269bb7d74aa1720922651529f3d3b2a787f5c60b73e5a0384950a4'; //gem Aalst
  const inzending = 'http://data.lblod.info/inzendingen-voor-toezicht/5E67C1A2A3ACB600080003B7';
  const besluitType = null; // 'http://data.lblod.info/DecisionType/5b3955cc006323233e711c482f3a6bf39a8d3eba6bbdb2c672bdfcf2b2985b03';
  const limit = null;
  const inzendingen = await getInzendingVoorToezichtToDo(formNode, bestuurseenheid, inzending, besluitType, limit);
  console.log(`Found ${inzendingen.length} for formNode ${formNode}`);
  return inzendingen;
}

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

export {
  getInzendingen,
  getSpecificInzending,
  getOneInzendingPerType
}
