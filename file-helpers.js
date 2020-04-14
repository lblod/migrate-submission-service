import { sparqlEscapeUri, sparqlEscapeString, sparqlEscapeDateTime, sparqlEscapeInt, uuid, update } from 'mu';
import fs from 'fs-extra';

/**
 * Write the given TTL content to a file and relates it to the given submitted document
 *
 * @param string ttl Turtle to write to the file
*/
async function insertTtlFile(fileGraph, formTtlFileUri, turtleContent, fileMetaDataNtriples) {
  const path = formTtlFileUri.replace('share://', '/share/');

  try {
    await fs.writeFile(path, turtleContent, 'utf-8');
  } catch (e) {
    console.log(`Failed to write TTL to file <${formTtlFileUri}>.`);
    throw e;
  }

  try {
    const stats = await fs.stat(path);
    const fileSize = stats.size;

    await update(`
      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX dbpedia: <http://dbpedia.org/ontology/>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      INSERT DATA {
        GRAPH ${sparqlEscapeUri(fileGraph)} {
          ${sparqlEscapeUri(formTtlFileUri)} nfo:fileSize ${sparqlEscapeInt(fileSize)}.
          ${fileMetaDataNtriples}
        }
      }
`);

  } catch (e) {
    console.log(`Failed to write TTL resource <${formTtlFileUri}> to triplestore.`);
    throw e;
  }

  return formTtlFileUri;
}

async function removeFile(file){
  console.log(`removeing file ${file}`);
  const path = file.replace('share://', '/share/');
  const content = await fs.remove(path);
  return content;
}

export {
  insertTtlFile,
  removeFile
}
