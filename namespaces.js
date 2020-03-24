import { Namespace } from 'rdflib';

const RDF = new Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#");
const FORM = new Namespace("http://lblod.data.gift/vocabularies/forms/");
const SHACL = new Namespace("http://www.w3.org/ns/shacl#");
const SKOS = new Namespace("http://www.w3.org/2004/02/skos/core#");
const XSD = new Namespace("http://www.w3.org/2001/XMLSchema#");

const SIGN = new Namespace("http://mu.semte.ch/vocabularies/ext/signing/");
const EXT = new Namespace("http://mu.semte.ch/vocabularies/ext/");
const PROV = new Namespace("http://www.w3.org/ns/prov#");
const BESLUIT = new Namespace("http://data.vlaanderen.be/ns/besluit#");
const MANDAAT = new Namespace("http://data.vlaanderen.be/ns/mandaat#");
const MU = new Namespace("http://mu.semte.ch/vocabularies/core/");
const DCT = new Namespace("http://purl.org/dc/terms/");
const SCHEMA = new Namespace("http://schema.org/");
const TOEZICHT = new Namespace("http://mu.semte.ch/vocabularies/ext/supervision/");
const MELDING = new Namespace("http://lblod.data.gift/vocabularies/automatische-melding/");
const NMO = new Namespace("http://www.semanticdesktop.org/ontologies/2007/03/22/nmo#");
const ADMS = new Namespace("http://www.w3.org/ns/adms#");
const NIE = new Namespace("http://www.semanticdesktop.org/ontologies/2007/01/19/nie#");
const ELOD = new Namespace("http://linkedeconomy.org/ontology#");
const ELI = new Namespace("http://data.europa.eu/eli/ontology#");
const RDFS = new Namespace('http://www.w3.org/2000/01/rdf-schema#');
const LBLODBESLUIT = new Namespace('http://lblod.data.gift/vocabularies/besluit/');
const PAV = new Namespace('http://purl.org/pav/');
const MEB = new Namespace('http://rdf.myexperiment.org/ontologies/base/');
const NFO = new Namespace('http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#');
const DBPEDIA = new Namespace('http://dbpedia.org/resource/');
export { RDF,
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
       }
