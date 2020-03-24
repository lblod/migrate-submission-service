import { uuid,
         query,
         update,
         sparqlEscapeUri,
         sparqlEscapeString,
         sparqlEscapeInt,
         sparqlEscapeDate,
         sparqlEscapeDateTime } from 'mu';
import request from 'request';

const ONGOING = 'http://lblod.data.gift/concepts/migrate-submission-service/status/ongoing';
const FINISHED = 'http://lblod.data.gift/concepts/migrate-submission-service/status/finished';
const FAILED = 'http://lblod.data.gift/concepts/migrate-submission-service/status/failed';
const DEFAULT_GRAPH = 'http://lblod.data.gift/resources/migrate-submission-service/graph/migration-graph';

async function getInzendingVoorToezichtToDo(formNodeUri){
  //TODO: filter on specific formNode
  const q = `
    PREFIX toezicht: <http://mu.semte.ch/vocabularies/ext/supervision/>
    PREFIX nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>

    SELECT ?graph ?inzendingUri WHERE {
      GRAPH ?graph {
        ?inzendingUri a toezicht:InzendingVoorToezicht.
        ?form <http://mu.semte.ch/vocabularies/ext/hasInzendingVoorToezicht> ?inzendingUri.
        ?form <http://mu.semte.ch/vocabularies/ext/hasForm> ${sparqlEscapeUri(formNodeUri)}.
        FILTER NOT EXISTS {
           ?task nuao:involves ?inzendingUri.
        }
      }
    }
  `;
  const results = parseResult(await query(q));
  return results;
}

async function createTaskForMigration(inzendingUri){
  let sUuid = uuid();
  let subject = `http://lblod.data.gift/resources/migrate-submission-service/task/${sUuid}`;
  let created = Date.now();

  let q = `
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX    nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>
    PREFIX    task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX    dct: <http://purl.org/dc/terms/>
    PREFIX    ndo: <http://oscaf.sourceforge.net/ndo.html#>
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(subject)} a task:Task;
          mu:uuid ${sparqlEscapeString(sUuid)};
          adms:status ${sparqlEscapeUri(ONGOING)};
          dct:created ${sparqlEscapeDateTime(created)};
          dct:modified ${sparqlEscapeDateTime(created)};
          dct:creator <http://lblod.data.gift/services/migrate-submission-service>;
          nuao:involves ${sparqlEscapeUri(inzendingUri)}.
      }
    }
  `;

  await query(q);
  const task = await getTask(subject);
  return task;
}

async function getTask(taskUri){
  const q  = `
      PREFIX    adms: <http://www.w3.org/ns/adms#>
      PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX    nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>
      PREFIX    task: <http://redpencil.data.gift/vocabularies/tasks/>
      PREFIX    dct: <http://purl.org/dc/terms/>
      PREFIX    ndo: <http://oscaf.sourceforge.net/ndo.html#>

      SELECT ?taskUri ?uuid ?status ?created ?modified ?involves ?creator WHERE {
         GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
            BIND(${sparqlEscapeUri(taskUri)} as ?taskUri)
            ?taskUri a task:Task;
              mu:uuid ?uuid;
              adms:status ?status;
              dct:created ?created;
              dct:modified ?modified;
              dct:creator ?creator;
              nuao:involves ?involves.
         }
      }
   `;

  const tasks = parseResult(await query(q));
  return tasks.length ? tasks[0] : null;
}

async function updateTask(uri, numberOfRetries, newStatusUri){
  let q = `
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    task: <http://redpencil.data.gift/vocabularies/tasks/>
    DELETE {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(uri)} adms:status ?status;
                                task:numberOfRetries ?numberOfRetries.
      }
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(uri)} adms:status ?status;
                                task:numberOfRetries ?numberOfRetries.
      }
    }
    ;
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(uri)} adms:status ${sparqlEscapeUri(newStatusUri)};
                                task:numberOfRetries ${sparqlEscapeInt(numberOfRetries)}.
      }
    }
  `;
  await query(q);
}

async function constructInzendingContentTtl(inzendingUri){
  const q = `
    PREFIX sign: <http://mu.semte.ch/vocabularies/ext/signing/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
    PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX schema: <http://schema.org/>
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX toezicht: <http://mu.semte.ch/vocabularies/ext/supervision/>
    PREFIX melding: <http://lblod.data.gift/vocabularies/automatische-melding/>
    PREFIX nmo: <http://nomisma.org/ontology.rdf#>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
    PREFIX elod: <http://linkedeconomy.org/ontology#>
    PREFIX eli: <http://data.europa.eu/eli/ontology#>

    CONSTRUCT {
      ?inzending a toezicht:InzendingVoorToezicht.
      ?inzending mu:uuid ?uuid.
      ?inzending dct:created ?created.
      ?inzending dct:modified ?modified.
      ?inzending nmo:sentDate ?sentDate.
      ?inzending nmo:receivedDate ?receivedDate.
      ?inzending dct:description ?description.
      ?inzending ext:remark ?remark.
      ?inzending toezicht:temporalCoverage ?temporalCoverage.
      ?inzending toezicht:businessIdentifier ?businessIdentifier.
      ?inzending toezicht:businessName ?businessName.
      ?inzending toezicht:dateOfEntryIntoForce ?datOfEntryIntoForce.
      ?inzending toezicht:endDate ?endDate.
      ?inzending toezicht:hasExtraTaxRates ?hasExtraTaxRates.
      ?inzending toezicht:agendaItemCount ?agendaItemCount.
      ?inzending toezicht:sessionDate ?sessionDate.
      ?inzending toezicht:decisionDateOtherAdministration ?decisionDateOtherAdministration.
      ?inzending toezicht:decisionSummary ?decisionSummary.
      ?inzending toezicht:dateHandover ?dateHandover.
      ?inzending toezicht:text ?text.
      ?inzending toezicht:datePublicationWebapp ?datePublicationWebapp.
      ?inzending adms:status ?status.
      ?inzending ext:lastModifiedBy ?lastModifiedBy.
      ?inzending dct:subject ?subject.
      ?formSolution ext:hasInzendingVoorToezicht ?inzending.
      ?inzending dct:type ?type.
      ?inzending toezicht:decisionType ?decisionType.
      ?inzending toezicht:regulationType ?regulationType.
      ?inzending toezicht:decidedBy ?decidedBy.
      ?inzending toezicht:authenticityType ?authenticityType.
      ?inzending toezicht:accountAcceptanceStatus ?accountAcceptanceStatus.
      ?inzending toezicht:deliveryReportType ?deliveryReportType.
      ?inzending toezicht:fiscalPeriod ?fiscalPeriod.
      ?inzending toezicht:nomenclature ?nomenclature.
      ?inzending toezicht:taxType ?taxType.
      ?melding dct:subject ?inzending.
      ?inzending nie:hasPart ?file.
      ?inzending toezicht:taxRate ?taxRate.
      ?inzending toezicht:simplifiedTaxRate ?simplifiedTaxRate.
      ?simplifiedTaxRate toezicht:amoount ?simplififiedTaxRateAmount.
      ?inzending toezicht:fileAddress ?fileAddress.
      ?fileAddress ext:fileAddressCacheStatus ?fileAddressStatus.
      ?fileAddressStatus ext:fileAddressCacheStatusLabel ?fileAddressStatusLabel.
      ?fileAddress ext:fileAddress ?fileAddressUrl.
  }
  WHERE {
    GRAPH ?g {
      ?inzending a toezicht:InzendingVoorToezicht.

      OPTIONAL {
        ?inzending mu:uuid ?uuid.
      }
      OPTIONAL {
        ?inzending dct:created ?created.
      }
      OPTIONAL {
        ?inzending dct:modified ?modified.
      }
      OPTIONAL {
        ?inzending nmo:sentDate ?sentDate.
      }
      OPTIONAL {
        ?inzending nmo:receivedDate ?receivedDate.
      }
      OPTIONAL {
        ?inzending dct:description ?description.
      }
      OPTIONAL {
        ?inzending ext:remark ?remark.
      }
      OPTIONAL {
        ?inzending toezicht:temporalCoverage ?temporalCoverage.
      }
      OPTIONAL {
        ?inzending toezicht:businessIdentifier ?businessIdentifier.
      }
      OPTIONAL {
        ?inzending toezicht:businessName ?businessName.
      }
      OPTIONAL {
        ?inzending toezicht:dateOfEntryIntoForce ?datOfEntryIntoForce.
      }
      OPTIONAL {
        ?inzending toezicht:endDate ?endDate.
      }
      OPTIONAL {
        ?inzending toezicht:hasExtraTaxRates ?hasExtraTaxRates.
      }
      OPTIONAL {
        ?inzending toezicht:agendaItemCount ?agendaItemCount.
      }
      OPTIONAL {
        ?inzending toezicht:sessionDate ?sessionDate.
      }
      OPTIONAL {
        ?inzending toezicht:decisionDateOtherAdministration ?decisionDateOtherAdministration.
      }
      OPTIONAL {
        ?inzending toezicht:decisionSummary ?decisionSummary.
      }
      OPTIONAL {
        ?inzending toezicht:dateHandover ?dateHandover.
      }
      OPTIONAL {
        ?inzending toezicht:text ?text.
      }
      OPTIONAL {
        ?inzending toezicht:datePublicationWebapp ?datePublicationWebapp.
      }
      OPTIONAL {
        ?inzending adms:status ?status.
      }
      OPTIONAL {
        ?inzending ext:lastModifiedBy ?lastModifiedBy.
      }
      OPTIONAL {
        ?inzending dct:subject ?subject.
      }
      OPTIONAL {
        ?formSolution ext:hasInzendingVoorToezicht ?inzending.
      }
      OPTIONAL {
        ?inzending dct:type ?type.
      }
      OPTIONAL {
        ?inzending toezicht:decisionType ?decisionType.
      }
      OPTIONAL {
        ?inzending toezicht:regulationType ?regulationType.
      }
      OPTIONAL {
        ?inzending toezicht:decidedBy ?decidedBy.
      }
      OPTIONAL {
        ?inzending toezicht:authenticityType ?authenticityType.
      }
      OPTIONAL {
        ?inzending toezicht:accountAcceptanceStatus ?accountAcceptanceStatus.
      }
      OPTIONAL {
        ?inzending toezicht:deliveryReportType ?deliveryReportType.
      }
      OPTIONAL {
        ?inzending toezicht:fiscalPeriod ?fiscalPeriod.
      }
      OPTIONAL {
        ?inzending toezicht:nomenclature ?nomenclature.
      }
      OPTIONAL {
        ?inzending toezicht:taxType ?taxType.
      }
      OPTIONAL {
        ?melding dct:subject ?inzending.
      }
      OPTIONAL {
        ?inzending nie:hasPart ?file.
      }
      OPTIONAL {
        ?inzending toezicht:taxRate ?taxRate.
      }
      OPTIONAL {
        ?inzending toezicht:simplifiedTaxRate ?simplifiedTaxRate.
      }
      OPTIONAL {
        ?inzending toezicht:simplifiedTaxRate ?simplifiedTaxRate.
        ?simplifiedTaxRate toezicht:amoount ?simplififiedTaxRateAmount.
      }
      OPTIONAL {
        ?inzending toezicht:fileAddress ?fileAddress.
        ?fileAddress ext:fileAddressCacheStatus ?fileAddressStatus.
        ?fileAddressStatus ext:fileAddressCacheStatusLabel ?fileAddressStatusLabel.
        ?fileAddress ext:fileAddress ?fileAddressUrl.
      }
    }
    FILTER( ${sparqlEscapeUri(inzendingUri)} = ?inzending )
  }
  `;
  const results = await constructQuery(q);
  return results;
}


async function insertData(graph, nTriples){
  //Note: nTriples is just a string.
  const q = `
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(graph)}{
        ${nTriples}
      }
    }
  `;
  await query(q);
}

async function linkSubmissionDocumentWithTtl(fileGraph, file, submissionDocument){
  const fileType = 'http://data.lblod.gift/concepts/form-data-file-type';
  await update(`
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    INSERT {
      GRAPH ?g {
        ${sparqlEscapeUri(submissionDocument)} dct:source ${sparqlEscapeUri(file)} .
      }
      GRAPH ${sparqlEscapeUri(fileGraph)} {
        ${sparqlEscapeUri(file)} dct:type ${sparqlEscapeUri(fileType)} .
      }
    } WHERE {
      GRAPH ?g {
        ${sparqlEscapeUri(submissionDocument)} a ext:SubmissionDocument .
      }
    }
  `);
}

/**
 * convert results of select query to an array of objects.
 * courtesy: Niels Vandekeybus
 * @method parseResult
 * @return {Array}
 */
function parseResult( result ) {
  const bindingKeys = result.head.vars;
  return result.results.bindings.map((row) => {
    const obj = {};
    bindingKeys.forEach((key) => obj[key] = row[key].value);
    return obj;
  });
};

/**
 * courtesy: Erika Pauwels
 * (but changed a little to work with virtuoso)
 */
async function constructQuery(query) {
  const format = 'text/turtle';
  const options = {
    method: 'POST',
    url: process.env.MU_SPARQL_ENDPOINT,
    headers: {
      'Accept': format
    },
    form: {
      query: query
    }
  };

  return new Promise ( (resolve,reject) => {
    return request(options, function(error, response, body) {
      if (error)
        reject(error);
      else
        resolve(body);
    });
  });
}

export { getInzendingVoorToezichtToDo,
         createTaskForMigration,
         updateTask,
         constructInzendingContentTtl,
         insertData,
         ONGOING,
         FINISHED,
         FAILED
       }
