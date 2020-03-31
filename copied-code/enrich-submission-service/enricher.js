import { sparqlEscapeUri, uuid } from 'mu';
import { writeToFile } from './graph-helpers';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { NamedNode, graph as rdflibGraph, parse as rdflibParse, namedNode, serialize, literal } from 'rdflib';
import request from 'request';

const BESTUURSORGAAN_SELECT_CONCEPT_SCHEME = 'http://data.lblod.info/concept-schemes/481c03f0-d07f-424e-9c2b-8d4cfb141c72';
const TOEZICHT_CONCEPT_SCHEMES = [
  'http://lblod.data.gift/concept-schemes/c93ccd41-aee7-488f-86d3-038de890d05a', // reglementtype
  'http://lblod.data.gift/concept-schemes/71e6455e-1204-46a6-abf4-87319f58eaa5', // type dossier voor inzending
  'http://lblod.data.gift/concept-schemes/5cecec47-ba66-4d7a-ac9d-a1e7962ca4e2', // document authenticity type
  'http://lblod.data.gift/concept-schemes/b65b15ba-6755-4cd2-bd07-2c2cf3c0e4d3', // MAR codes
  'http://lblod.data.gift/concept-schemes/3037c4f4-1c63-43ac-bfc4-b41d098b15a6' // tax type
];

/**
 * Enrich the harvested triples dataset with derived knowledge
 * based on the current harvested triples and the data in the triplestore.
*/
export default async function enrich(submissionDocument) {
  const tmpGraph = `http://lblod.data.gift/services/enrich-submission-service/${uuid()}`;
  const store = rdflibGraph();
  for (let conceptScheme of TOEZICHT_CONCEPT_SCHEMES) {
    const triples = await constructConceptScheme(conceptScheme);
    if(triples){
      rdflibParse( triples, store, tmpGraph, 'text/turtle' );
    }
  }

  const expandedTriples = await expandDocumentType(submissionDocument);
  if(expandedTriples){
    rdflibParse( expandedTriples, store, tmpGraph, 'text/turtle' );
  }
  const bestuursorganenTriples = await addBestuursorganenForBestuurseenheid(submissionDocument);
  if(bestuursorganenTriples){
    rdflibParse( bestuursorganenTriples, store, tmpGraph, 'text/turtle' );
  }

  const enrichments = serialize(namedNode(tmpGraph), store, undefined, 'application/n-triples');
  return enrichments;
}

async function constructConceptScheme(conceptScheme) {
  console.log(`Add concept scheme ${conceptScheme} to meta graph`);
  const q = `
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    CONSTRUCT {
        ?concept ?p ?o .
    } WHERE {
      GRAPH ?g {
        ?concept skos:inScheme ${sparqlEscapeUri(conceptScheme)} ;
          ?p ?o .
      }
    }
  `;

  return await constructQuery(q);
}

/**
 * Enrich the harvested data with the broader document types
 *
 * E.g. a 'Belastingsreglement' is also a 'Reglement and verordening'
*/
async function expandDocumentType(submissionDocument) {
  // TODO get file URL of harvested file
  // TODO parse harvested TTL
  // TODO get all types and insert broader types  (see import-submission-service)
}

/**
 * Enrich the harvested data with the known bestuursorganen of the bestuurseenheid
 * that submitted the document
*/
async function addBestuursorganenForBestuurseenheid(submissionDocument) {
  console.log(`Add bestuursorganen for submission document ${submissionDocument} to meta graph`);
  const q = `
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX pav: <http://purl.org/pav/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
    PREFIX lblodlg: <http://data.lblod.info/vocabularies/leidinggevenden/>

    CONSTRUCT {
      ${sparqlEscapeUri(BESTUURSORGAAN_SELECT_CONCEPT_SCHEME)} a skos:ConceptScheme .
      ?bestuursorgaan besluit:bestuurt ?bestuurseenheid ;
        skos:prefLabel ?bestuursorgaanLabel ;
        besluit:classificatie ?bestuursorgaanClassificatie .
      ?bestuursorgaanInTijd mandaat:isTijdspecialisatieVan ?bestuursorgaan ;
        mandaat:bindingStart ?start ;
        mandaat:bindingEinde ?end ;
        skos:prefLabel ?botLabel;
        skos:inScheme ${sparqlEscapeUri(BESTUURSORGAAN_SELECT_CONCEPT_SCHEME)} .
    } WHERE {
      GRAPH ?g {
        ?submission dct:subject ${sparqlEscapeUri(submissionDocument)} ;
          pav:createdBy ?bestuurseenheid .
      }
      GRAPH <http://mu.semte.ch/graphs/public> {
        ?bestuursorgaan besluit:bestuurt ?bestuurseenheid ;
          skos:prefLabel ?bestuursorgaanLabel ;
          besluit:classificatie ?bestuursorgaanClassificatie .
        ?bestuursorgaanInTijd mandaat:isTijdspecialisatieVan ?bestuursorgaan ;
          mandaat:bindingStart ?start .

        OPTIONAL { ?bestuursorgaanInTijd mandaat:bindingEinde ?end . }

        BIND(CONCAT(?bestuursorgaanLabel, "( ", YEAR(?start), " - ", IF( BOUND(?end), YEAR(?end), ""), " )" ) as ?botLabel)

        FILTER NOT EXISTS {
          ?bestuursorgaanInTijd lblodlg:heeftBestuursfunctie ?leidinggevende .
        }
      }
    }
  `;
  return await constructQuery(q);
}


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
