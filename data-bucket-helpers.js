import { NamedNode, graph as rdflibGraph, parse as rdflibParse, namedNode, serialize, literal } from 'rdflib';
import { uuid } from 'mu';
import { readdirSync, readFileSync } from 'fs';
import { join } from 'path';
import { getBestuursorganenInTijd } from './queries';

import { RDF,
         FORM,
         SHACL,
         SKOS,
         XSD,
         SIGN,
         EXT,
         PROV,
         BESLUIT,
         MANDAAT,
         MU,
         MELDING,
         NMO,
         ADMS,
         NIE,
         ELOD,
         ELI,
         DCT,
         SCHEMA,
         TOEZICHT,
         RDFS,
         LBLODBESLUIT,
         PAV,
         MEB,
         NFO,
         DBPEDIA
       } from './namespaces';

async function createDataBuckets(inzendingData){
  const store = rdflibGraph();
  const sourceGraph = namedNode('http://source');
  const formTtlGraph = namedNode('http://target');
  const dbGraph = namedNode('http://dbGraph');
  const codeListsGraph = namedNode('http://codelists');
  const fileGraph = namedNode('http://mu.semte.ch/graphs/public');
  const remoteDataObjectGraph = namedNode('http://mu.semte.ch/graphs/public');
  loadCodelists('/app/codelists', store, codeListsGraph);
  rdflibParse( inzendingData, store, sourceGraph.value, 'text/turtle' );

  let inzendingVoorToezicht = store.match(undefined, RDF('type'), TOEZICHT('InzendingVoorToezicht'), sourceGraph)[0].subject;

  const formTtlFile = createFileTtlMetaData(store, fileGraph);

  const submission = extractSubmission(inzendingVoorToezicht, store, sourceGraph, codeListsGraph, dbGraph);

  const subissionDocument = extractSubmittedDocument(inzendingVoorToezicht, store, sourceGraph, codeListsGraph, dbGraph, submission, formTtlFile);
  await extractFormTtlData(inzendingVoorToezicht, store, sourceGraph, codeListsGraph, formTtlGraph, subissionDocument, remoteDataObjectGraph);
  const formData = extractFormData(inzendingVoorToezicht, store, formTtlGraph, codeListsGraph, dbGraph, submission, subissionDocument, formTtlFile);
  const nTriplesDbGraph = serialize(dbGraph, store, undefined, 'application/n-triples'); //application/n-triples
  const nTriplesFileGraph = serialize(fileGraph, store, undefined, 'application/n-triples'); //application/n-triples
  const turtleFormTtlContent = serialize(formTtlGraph, store, undefined, 'application/n-triples');
  const nTriplesRemoteDataObjectGraph= serialize(remoteDataObjectGraph, store, undefined, 'application/n-triples');
  return {
    store,
    sourceGraph,
    dbGraph,
    remoteDataObjectGraph,
    codeListsGraph,
    inzendingVoorToezicht,
    formTtlGraph,
    subissionDocument,
    submission,
    formTtlFile,
    formData,
    fileGraph,
    nTriplesDbGraph,
    nTriplesFileGraph,
    turtleFormTtlContent,
    nTriplesRemoteDataObjectGraph
  };
}

function createFileTtlMetaData(store, fileGraph){
  const id = uuid();
  const filename = `${id}.ttl`;
  const formTtlFile = namedNode(`share://submissions/${filename}`);
  const now = new Date();

  store.add(formTtlFile, MU('uuid'), id, fileGraph);
  store.add(formTtlFile, NFO('fileName'), filename, fileGraph);
  store.add(formTtlFile, DCT('creator'), namedNode('http://lblod.data.gift/services/migrate-submission-service'), fileGraph);
  store.add(formTtlFile, DCT('created'), now, fileGraph);
  store.add(formTtlFile, DCT('modified'), now, fileGraph);
  store.add(formTtlFile, DCT('format'), 'text/turtle', fileGraph);
  store.add(formTtlFile, DCT('format'), 'text/turtle', fileGraph);
  store.add(formTtlFile, DBPEDIA('fileExtension'), 'ttl', fileGraph);
  store.add(formTtlFile, DCT('type'), namedNode('http://data.lblod.gift/concepts/form-data-file-type'), fileGraph);
  return formTtlFile;
}

function extractSubmission(inzendingVoorToezicht, store, sourceGraph, codeListsGraph, submissionGraph){
  //These triples will result to resources in the DB
  let newUuid = uuid();
  const newSubmission = namedNode(`http://data.lblod.info/submissions/${newUuid}`); //Note: will be reused
  store.add(newSubmission, MU('uuid'), newUuid, submissionGraph);
  store.add(newSubmission, RDF('type'), MEB('Submission'), submissionGraph);

  mapPredicateToNewSubject(store, sourceGraph, DCT('subject'),
                           submissionGraph, newSubmission, PAV('createdBy'));

  mapPredicateToNewSubject(store, sourceGraph, DCT('created'),
                           submissionGraph, newSubmission, DCT('created'));

  mapPredicateToNewSubject(store, sourceGraph, DCT('modified'),
                           submissionGraph, newSubmission, DCT('modified'));

  mapPredicateToNewSubject(store, sourceGraph, NMO('sentDate'),
                           submissionGraph, newSubmission, NMO('sentDate'));

  mapPredicateToNewSubject(store, sourceGraph, EXT('lastModifiedBy'),
                           submissionGraph, newSubmission, EXT('lastModifiedBy'));

  mapPredicateToNewSubject(store, sourceGraph, NMO('receivedDate'),
                           submissionGraph, newSubmission, NMO('receivedDate'));

  const subStatus = store.match(inzendingVoorToezicht, ADMS('status'), undefined, sourceGraph)[0].object;
  store.add(newSubmission, ADMS('status'), getNewCodeListEquivalent(store, codeListsGraph, subStatus), submissionGraph);
  store.add(newSubmission, DCT('source'), inzendingVoorToezicht, submissionGraph);
  const files = store.match(inzendingVoorToezicht, NIE('hasPart'), undefined, sourceGraph);
  files.forEach(file => {
    store.add(newSubmission, NIE('hasPart'), file.object, submissionGraph);
  });
  return newSubmission;
}

function extractSubmittedDocument(inzendingVoorToezicht, store, sourceGraph, codeListsGraph, targetGraph, submission, formTtlFile){
  //These triples will result to resources in the DB
  let newUuid = uuid();
  const newSubDoc = namedNode(`http://data.lblod.info/submission-documents/${newUuid}`); //Note: will be reused
  store.add(newSubDoc, MU('uuid'), newUuid, targetGraph);
  store.add(newSubDoc, RDF('type'), EXT('SubmissionDocument'), targetGraph);
  store.add(newSubDoc, DCT('source'), formTtlFile, targetGraph);
  //the links need to be there too
  const files = store.match(inzendingVoorToezicht, NIE('hasPart'), undefined, sourceGraph);
  files.forEach(file => {
    store.add(newSubDoc, DCT('hasPart'), file.object, targetGraph);
  });

  store.add(submission, DCT('subject'), newSubDoc, targetGraph);
  return newSubDoc;
}

async function extractFormTtlData(inzendingVoorToezicht, store, sourceGraph, codeListsGraph, targetGraph, newSubDoc, publicGraph){
  mapPredicateToNewSubject(store, sourceGraph, DCT('description'),
                           targetGraph, newSubDoc, DCT('description'));

  mapPredicateToNewSubject(store, sourceGraph, EXT('remark'),
                           targetGraph, newSubDoc, RDFS('comment'));

  mapPredicateToNewSubject(store, sourceGraph, TOEZICHT('temporalCoverage'),
                           targetGraph, newSubDoc, ELOD('financialYear'));

  mapPredicateToNewSubject(store, sourceGraph, TOEZICHT('temporalCoverage'),
                           targetGraph, newSubDoc, ELOD('financialYear'));

  //TODO: should be conditoinal
  const newOrg = namedNode(`http://data.lblod.info/organisations/${uuid()}`);
  store.add(newOrg, RDF('type'), EXT('Organization'), targetGraph);
  store.add(newSubDoc, ELI('is_about'), newOrg, targetGraph);

  mapPredicateToNewSubject(store, sourceGraph, TOEZICHT('businessIdentifier'),
                           targetGraph, newOrg, DCT('identifier'));
  mapPredicateToNewSubject(store, sourceGraph, TOEZICHT('businessName'),
                           targetGraph, newOrg, SKOS('prefLabel'));

  mapPredicateToNewSubject(store, sourceGraph, TOEZICHT('dateOfEntryIntoForce'),
                           targetGraph, newSubDoc, ELI('first_date_entry_in_force'));

  mapPredicateToNewSubject(store, sourceGraph, TOEZICHT('endDate'),
                           targetGraph, newSubDoc, ELI('date_no_longer_in_force'));

  mapPredicateToNewSubject(store, sourceGraph, TOEZICHT('hasExtraTaxRates'),
                           targetGraph, newSubDoc, LBLODBESLUIT('hasAdditionalTaxRate'));

  const newZitting = namedNode(`http://data.lblod.info/zittingen/${uuid()}`);
  store.add(newZitting, RDF('type'), EXT('PlaceholderZiting'), targetGraph); //To keep track easily these are actually not real Zittingen
  store.add(newZitting, RDF('type'), BESLUIT('Zitting'), targetGraph);

  let zittingsDatum = (store.match(inzendingVoorToezicht, TOEZICHT('sessionDate'), undefined, sourceGraph)[0] || {}).object;
  if(zittingsDatum && zittingsDatum.value){
    const updatedDate = new Date(zittingsDatum.value);
    updatedDate.setHours( 18 ); //This is a best guess  as discussed by Erika. It used to be date, now it is datetime.
    const newValue = literal(updatedDate.toISOString(), XSD('dateTime'));
    store.add(newZitting, PROV('startedAtTime'), newValue , targetGraph);
  }

  //Linking zitting to the submissionDocument is depending on the type of document is submitted.
  const typeInzending = store.match(inzendingVoorToezicht, TOEZICHT('decisionType'), undefined, sourceGraph)[0].object;

  const oldAgenda = namedNode('http://data.lblod.info/DecisionType/30acafe57031815d94b54181c2040a075ffc7d16d93166aea8ad6e8e829ee696');
  const oldBesluitenlijst = namedNode('http://data.lblod.info/DecisionType/46b254008bbca1354e632dc40cf550c6b313e523799cafd7200a48a19e09249f');
  const oldNotulen = namedNode('http://data.lblod.info/DecisionType/5a71a9e79b58c6b095cb2e575c7a397cc1fe80385e4d1deddd66745a03638f9f');
  if(typeInzending.equals(oldAgenda)){
     store.add(newSubDoc, RDF('type'), getNewCodeListEquivalent(store, codeListsGraph, oldAgenda), targetGraph);
    store.add(newZitting, BESLUIT('heeftAgenda'), newSubDoc, targetGraph);
  }
  else if(typeInzending.equals(oldBesluitenlijst)){
    store.add(newSubDoc, RDF('type'), getNewCodeListEquivalent(store, codeListsGraph, oldBesluitenlijst), targetGraph);
    store.add(newZitting, BESLUIT('heeftBesluitenlijst'), newSubDoc, targetGraph);
  }
  else if(typeInzending.equals(oldNotulen)){
    store.add(newSubDoc, RDF('type'), getNewCodeListEquivalent(store, codeListsGraph, oldNotulen), targetGraph);
    store.add(newZitting, BESLUIT('heeftNotulen'), newSubDoc, targetGraph);
  }
  else {
    const targetObject = getNewCodeListEquivalent(store, codeListsGraph, typeInzending);
    if(!targetObject){
      throw new Error(`No codelist equivalent found for ${typeInzending.value}, inzending: ${inzendingVoorToezicht.value}`);
    }
    store.add(newSubDoc, RDF('type'), targetObject, targetGraph);
    // a very complex path needs to be generated
    const bap = namedNode(`http://data.lblod.info/behandeling-van-agendapunten/${uuid()}`);
    const ap = namedNode(`http://data.lblod.info/agendapunten/${uuid()}`);
    store.add(bap, RDF('type'), BESLUIT('BehandelingVanAgendapunt'), targetGraph);
    store.add(bap, RDF('type'), EXT('PlaceholderBehandelingVanAgendapunt'), targetGraph);
    store.add(bap, DCT('subject'), ap, targetGraph);
    store.add(ap, RDF('type'), BESLUIT('Agendapunt'), targetGraph);
    store.add(ap, RDF('type'), EXT('Agendapunt'), targetGraph);
    store.add(newZitting, BESLUIT('behandelt'), ap, targetGraph);
    store.add(bap, PROV('generated'), newSubDoc, targetGraph);
  }

  mapPredicateToNewSubject(store, sourceGraph, TOEZICHT('datePublicationWebapp'),
                           targetGraph, newSubDoc, ELI('date_publication'));

  const regulationType = store.match(inzendingVoorToezicht, TOEZICHT('regulationType'), undefined, sourceGraph)[0];
  if(regulationType){
    store.add(newSubDoc, RDF('type'),
              getNewCodeListEquivalent(store, codeListsGraph, regulationType.object), targetGraph);
  }

  const bestuursOrgaanInTijd = await deduceBestuursorgaanInTijd(store, sourceGraph, inzendingVoorToezicht);
  if(bestuursOrgaanInTijd){
    //How bestuursorgaan in tijd is connnect depends on type of document
    const typeInzending = store.match(inzendingVoorToezicht, TOEZICHT('decisionType'), undefined, sourceGraph)[0].object;
    const oldNotulen = namedNode('http://data.lblod.info/DecisionType/5a71a9e79b58c6b095cb2e575c7a397cc1fe80385e4d1deddd66745a03638f9f');
    if(typeInzending.equals(oldNotulen)){
      store.add(newZitting, BESLUIT('heeftNotulen'), newSubDoc, targetGraph);
      store.add(newZitting, BESLUIT('isGehoudenDoor'), namedNode(bestuursOrgaanInTijd.botUri), targetGraph);
    }
    else {
      store.add(newSubDoc, ELI('passed_by'), namedNode(bestuursOrgaanInTijd.botUri), targetGraph);
    }
  }

  const oldAuthenticity = store.match(inzendingVoorToezicht, TOEZICHT('authenticityType'), undefined, sourceGraph);
  if(oldAuthenticity.length){
    store.add(newSubDoc, LBLODBESLUIT('authenticityType'), getNewCodeListEquivalent(store, codeListsGraph, oldAuthenticity[0].object), targetGraph);
  }

  const oldChartOfAccount = store.match(inzendingVoorToezicht, TOEZICHT('nomenclature'), undefined, sourceGraph);
  if(oldChartOfAccount.length){
    store.add(newSubDoc, LBLODBESLUIT('chartOfAccount'), getNewCodeListEquivalent(store, codeListsGraph, oldChartOfAccount[0].object), targetGraph);
  }

  const oldTaxType = store.match(inzendingVoorToezicht, TOEZICHT('taxType'), undefined, sourceGraph);
  if(oldTaxType.length){
    store.add(newSubDoc, LBLODBESLUIT('taxType'), getNewCodeListEquivalent(store, codeListsGraph, oldTaxType[0].object), targetGraph);
  }

  const files = store.match(inzendingVoorToezicht, NIE('hasPart'), undefined, sourceGraph);
  files.forEach(file => {
    store.add(newSubDoc, DCT('hasPart'), file.object, targetGraph);
    store.add(file.object, RDF('type'), namedNode('http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#FileDataObject'), targetGraph);
  });

  const fileAddresses = store.match(inzendingVoorToezicht, TOEZICHT('fileAddress'), undefined, sourceGraph);
  fileAddresses.forEach(url => {
    const addUuid = uuid();
    const newFileAdd =  namedNode(`http://data.lblod.info/remote-data-objects/${addUuid}`);
    store.add(newSubDoc, DCT('hasPart'), newFileAdd, targetGraph);
    store.add(newFileAdd, RDF('type'), namedNode('http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#RemoteDataObject'), targetGraph);
    //Database and localstore keeps track of the url
    store.add(newFileAdd, MU('uuid'), addUuid, publicGraph);
    store.add(newFileAdd, RDF('type'), namedNode('http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#FileDataObject'), publicGraph);
    store.add(newFileAdd, RDF('type'), namedNode('http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#RemoteDataObject'), publicGraph);
    const address = store.match(url.object, EXT('fileAddress'), undefined, sourceGraph);
    if(address.length){
       // So n-triples charcaters seem to be escaped for special ones. E.g. \u0027
      // SPARQL expects 'decoded/unescaped' data. It won't interpret  \u0027. So we have to do the conversion ourselves
      const unescapedAddress = JSON.parse(`"${address[0].object.value}"`);
      store.add(newFileAdd, namedNode('http://www.semanticdesktop.org/ontologies/2007/01/19/nie#url'), unescapedAddress, publicGraph);
      store.add(newFileAdd, namedNode('http://www.semanticdesktop.org/ontologies/2007/01/19/nie#url'), unescapedAddress, targetGraph);

      const cacheStatus = store.match(url.object, EXT('fileAddressCacheStatus'), undefined, sourceGraph)[0].object;
      const status = store.match(cacheStatus, EXT('fileAddressCacheStatusLabel'), undefined, sourceGraph)[0].object;

      if(status.value == "dead" || status.value == "failed"){
        store.add(newFileAdd, ADMS('status'), namedNode('http://lblod.data.gift/file-download-statuses/failure'), publicGraph);
      }
      else{
        const cachedLogicalFile = store.match(undefined, NIE('dataSource'), url.object, sourceGraph)[0];
        if(!cachedLogicalFile) return;
        const cachedPhysFile = store.match(undefined, NIE('dataSource'), cachedLogicalFile.subject, sourceGraph)[0];
        if(!cachedPhysFile) return;
        store.add(cachedPhysFile.subject, NIE('dataSource'), newFileAdd ,publicGraph);
        store.add(newFileAdd, ADMS('status'), namedNode('http://lblod.data.gift/file-download-statuses/success'), publicGraph);
      }
    }
  });

  //Note: in the data <http://mu.semte.ch/vocabularies/ext/supervision/taxRate> has not been found (expect for dangling stuff)
  const simpleTaxRates = store.match(inzendingVoorToezicht, TOEZICHT('simplifiedTaxRate'), undefined, sourceGraph);
  if(simpleTaxRates.length){
    const newTaxRate = namedNode(`http://data.lblod.info/tax-rates/${uuid()}`);
    store.add(newTaxRate, RDF('type'), LBLODBESLUIT('TaxRate'), targetGraph);
    store.add(newSubDoc, LBLODBESLUIT('taxRate'), newTaxRate, targetGraph);


    simpleTaxRates.forEach(r => {
      const amount = store.match(r.object, TOEZICHT('amoount'), undefined, sourceGraph);
      if(amount.length){
        store.add(newTaxRate, SCHEMA('price'), amount[0].object, targetGraph);
      }
    });
  }
}

function extractFormData(inzendingVoorToezicht, store, sourceGraph, codeListsGraph, targetGraph, submission, submissionDocument, formTtlFile){
  //this extracts the flattened resrouce
  let newUuid = uuid();
  const formData = namedNode(`http://data.lblod.info/form-datas/${newUuid}`); //Note: will be reused
  store.add(formData, MU('uuid'), newUuid, targetGraph);
  store.add(formData, RDF('type'), MELDING('FormData'), targetGraph);
  store.add(formData, PROV('hadPrimary'), formTtlFile, targetGraph);
  store.add(submission, PROV('generated'), formData, targetGraph);

  mapPredicateToNewSubject(store, sourceGraph, RDF('type'),
                           targetGraph, formData, DCT('type'), submissionDocument);

  mapPredicateToNewSubject(store, sourceGraph, ELI('date_publication'),
                           targetGraph, formData, ELI('date_publication'));

  mapPredicateToNewSubject(store, sourceGraph, ELI('passed_by'),
                           targetGraph, formData, ELI('passed_by'));

  mapPredicateToNewSubject(store, sourceGraph, BESLUIT('isGehoudenDoor'),
                           targetGraph, formData, ELI('passed_by'));

  mapPredicateToNewSubject(store, sourceGraph, ELI('is_about'),
                           targetGraph, formData, ELI('is_about'));

  mapPredicateToNewSubject(store, sourceGraph, ELOD('financialYear'),
                           targetGraph, formData, ELOD('financialYear'));

  mapPredicateToNewSubject(store, sourceGraph, ELI('first_date_entry_in_force'),
                           targetGraph, formData, ELI('first_date_entry_in_force'));

  mapPredicateToNewSubject(store, sourceGraph, ELI('date_no_longer_in_force'),
                           targetGraph, formData, ELI('date_no_longer_in_force'));

  mapPredicateToNewSubject(store, sourceGraph, LBLODBESLUIT('authenticityType'),
                         targetGraph, formData, LBLODBESLUIT('authenticityType'));

  mapPredicateToNewSubject(store, sourceGraph, LBLODBESLUIT('chartOfAccount'),
                           targetGraph, formData, LBLODBESLUIT('chartOfAccount'));

  mapPredicateToNewSubject(store, sourceGraph, LBLODBESLUIT('taxRate'),
                           targetGraph, formData, LBLODBESLUIT('taxRate'));

  mapPredicateToNewSubject(store, sourceGraph, LBLODBESLUIT('taxType'),
                           targetGraph, formData, LBLODBESLUIT('taxType'));

  mapPredicateToNewSubject(store, sourceGraph, LBLODBESLUIT('hasAdditionalTaxRate'),
                           targetGraph, formData, LBLODBESLUIT('hasAdditionalTaxRate'));

  mapPredicateToNewSubject(store, sourceGraph, DCT('description'),
                           targetGraph, formData, DCT('description'));

  mapPredicateToNewSubject(store, sourceGraph, RDFS('comment'),
                           targetGraph, formData, RDFS('comment'));

  mapPredicateToNewSubject(store, sourceGraph, DCT('hasPart'),
                           targetGraph, formData, DCT('hasPart'));

   mapPredicateToNewSubject(store, sourceGraph, SCHEMA('price'),
                         targetGraph, formData, EXT('taxRateAmount'));

  mapPredicateToNewSubject(store, sourceGraph, PROV('startedAtTime'),
                           targetGraph, formData, EXT('sessionStartedAtTime'));

  return formData;
}

function loadCodelists(folder, store, targetGraph){
  readdirSync(folder).forEach(f => {
    const data = readFileSync(join(folder, f));
    rdflibParse( data.toString(), store, targetGraph.value, 'text/turtle' );
  });
}

function getNewCodeListEquivalent(store, graph, oldEntry){
  const entries = store.match(undefined, SKOS('exactMatch'), oldEntry, graph);
  if(!entries.length) return null;
  return entries[0].subject;
}

function mapPredicateToNewSubject(store, graph, oldPredicate, targetGraph,
                                  targetSubject, targetPredicate, optionalSourceSubject = null){
  const triples = store.match(optionalSourceSubject, oldPredicate, undefined, graph);

  const updatedTriples = triples.map(t => {
    const newTriple = {
      subject: targetSubject,
      predicate: targetPredicate,
      object: t.object,
      graph: targetGraph
    };
    return newTriple;
  } );
  updatedTriples.forEach(t => store.add(t));
}

async function deduceBestuursorgaanInTijd(store, graph, inzending){
  const bestuursorgaan = (store.match(inzending, TOEZICHT('decidedBy'), undefined, graph)[0] || {}).object;
  if(!bestuursorgaan) return null;

  let zittingsDatum = (store.match(inzending, TOEZICHT('sessionDate'), undefined, graph)[0] || {}).object;
  if(!zittingsDatum || !zittingsDatum.value) return null;
  zittingsDatum = new Date(zittingsDatum.value);

  const bots = await getBestuursorganenInTijd(bestuursorgaan.value);
  const bot = bots.find(b => {
    const start = new Date(b.start);
    const end = b.end ? new Date(b.end) : null;
    if(zittingsDatum < start){
      return false;
    }
    if(zittingsDatum >= start && !end){
      return true;
    }
    if(zittingsDatum >= start && zittingsDatum <= end){
      return true;
    }
    return false;
  });
  return bot;
}


export { createDataBuckets }
