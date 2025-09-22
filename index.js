/**
 * MongoPackage
 * ------------
 * Questo package incapsula funzioni comuni per interagire con MongoDB tramite Mongoose.
 * È pensato per essere riutilizzabile in vari contesti, permettendo di collegarsi al DB,
 * inserire documenti, verificare esistenza, cancellare, trovare o aggregare dati.
 * 
 * Supporta anche:
 * - Inserimenti condizionali (upsert)
 * - Inserimento in batch (insertArray)
 * - Disconnessione esplicita dal database (disconnect)
 * - Operazioni personalizzabili tramite modello dinamico o predefinito
 *
 * Uso base:
 * ---------
 * const MyModel = require('./models/MyModel');
 * const MongoPackage = require('./MongoPackage');
 * 
 * const db = new MongoPackage({
 *   mongoUri: 'mongodb://localhost:27017/mio-db',
 *   model: MyModel, // opzionale: modello predefinito
 * });
 * 
 * // Inserimento semplice
 * const nuovoItem = await db.insertItem({ _id: '123', nome: 'Test' });
 * 
 * // Inserimento con update se esiste già
 * const upserted = await db.insertItem({ _id: '123', nome: 'Aggiornato' }, true);
 * 
 * // Verifica esistenza
 * const esiste = await db.existsItem({ _id: '123' }); // true/false
 * 
 * // Inserimento array di item
 * await db.insertArray([{ _id: 'a' }, { _id: 'b' }], true);
 * 
 * // Cancellazione multipla
 * await db.deleteItems(['123', 'a']);
 * 
 * // Ricerca con query
 * const risultati = await db.findItems({ nome: 'Test' }, { limit: 10 });
 * 
 * // Aggregazione
 * const aggregati = await db.runAggregation([
 *   { $match: { campo: 'valore' } },
 *   { $group: { _id: '$altroCampo', count: { $sum: 1 } } }
 * ]);
 * 
 * // Esempio di updateManyField
 * await db.updateManyField(
 *   { currentseason: '2025' },  // filtro
 *   'update_at',                // campo custom
 *   new Date(),                 // valore
 *   MyModel                     // modello (opzionale se già impostato di default)
 * );
 * 
 * // Disconnessione dal DB
 * await db.disconnect();
 * 
 * Esempi avanzati di utilizzo:
 * ----------------------------
 * // Utilizzo di un modello alternativo per una singola operazione
 * const AltroModel = require('./models/AltroModel');
 * const altroRisultato = await db.insertItem({ nome: 'Altro' }, false, AltroModel);
 * 
 * // Query "raw" su una collection senza schema Mongoose
 * const docs = await db.queryCollection('utenti', { attivo: true }, { limit: 5, sort: { nome: 1 } });
 * 
 * // Aggregazione "raw" su una collection senza schema
 * const agg = await db.aggregateCollection('ordini', [
 *   { $match: { stato: 'spedito' } },
 *   { $group: { _id: '$cliente', totale: { $sum: '$importo' } } }
 * ]);
 * 
 * Note:
 * -----
 * - È possibile passare un model alternativo ad ogni metodo, utile se si lavora con più collezioni.
 * - In caso di errore, i metodi loggano l’errore e lo rilanciano.
 */


const mongoose = require('mongoose');

class MongoPackage {
  /**
   * Inizializza il package Mongo con URI e modello opzionale.
   * @param {Object} settings - Impostazioni di connessione e modello.
   * @param {string} settings.mongoUri - URI di connessione MongoDB.
   * @param {mongoose.Model} [settings.model] - Modello Mongoose predefinito.
   */
  constructor(settings) {
    if (!settings.mongoUri) {
      throw new Error("Missing 'mongoUri' in settings");
    }

    this.mongoUri = settings.mongoUri;
    this.defaultModel = settings.model || null;

    this.connect();
  }

  /**
   * Connette Mongoose al database MongoDB.
   */
  async connect() {
    try {
      await mongoose.connect(this.mongoUri);
      console.log('Connected to MongoDB');
    } catch (error) {
      console.error('Error connecting to MongoDB:', error);
      throw error;
    }
  }

  /**
   * Restituisce il modello da usare (predefinito o passato al metodo).
   * @param {mongoose.Model|null} model - Modello opzionale.
   * @returns {mongoose.Model}
   */
  getModel(model) {
    if (!model && !this.defaultModel) {
      throw new Error("No model specified or default model provided");
    }
    return model || this.defaultModel;
  }
    
  /**
   * Inserisce un singolo documento, con possibilità di upsert.
   * @param {Object} item - Documento da inserire.
   * @param {boolean} [updateIfExists=false] - Se true, esegue upsert.
   * @param {mongoose.Model|null} [model=null] - Modello alternativo.
   * @returns {Promise<Object>} Documento inserito o aggiornato.
   */
  async insertItem(item, updateIfExists = false, model = null) {
    try {
      const activeModel = this.getModel(model);

      if (updateIfExists) {
        const result = await activeModel.findByIdAndUpdate(
          item._id,
          { $set: item }, // ✅ assicura aggiornamento completo anche di campi annidati
          {
            new: true,
            upsert: true,
          }
        );
        return result;
      } else {
        const newItem = new activeModel(item);
        await newItem.save();
        return newItem;
      }
    } catch (error) {
      console.error('Error inserting item:', error);
      throw error;
    }
  }


  /**
   * Verifica se esiste almeno un documento che soddisfa la query.
   * @param {Object} query - Filtro di ricerca.
   * @param {mongoose.Model|null} [model=null] - Modello alternativo.
   * @returns {Promise<boolean>} true se esiste almeno un match.
   */
  async existsItem(query, model = null) {
    try {
      const activeModel = this.getModel(model);
      const exists = await activeModel.exists(query);
      return !!exists;
    } catch (error) {
      console.error('Error checking item existence:', error);
      throw error;
    }
  }

  /**
   * Inserisce un array di documenti, con possibilità di upsert.
   * @param {Array<Object>} items - Array di documenti da inserire.
   * @param {boolean} [updateIfExists=false] - Se true, usa upsert per ogni item.
   * @param {mongoose.Model|null} [model=null] - Modello alternativo.
   * @returns {Promise<Array>} Array dei risultati.
   */
  async insertArray(items, updateIfExists = false, model = null) {
    try {
      if (!items || items.length === 0) {
        console.log('insertArray.items empty');
        return [];
      }

      const results = [];
      for (const item of items) {
        const result = await this.insertItem(item, updateIfExists, model);
        results.push(result);
      }
      return results;
    } catch (error) {
      console.error('Error inserting array:', error);
      throw error;
    }
  }

  /**
   * Cancella documenti dato un array di `_id`.
   * @param {Array<string>} ids - Array di ID da cancellare.
   * @param {mongoose.Model|null} [model=null] - Modello alternativo.
   * @returns {Promise<Object>} Risultato dell'operazione.
   */
  async deleteItems(ids, model = null) {
    try {
      if (!Array.isArray(ids)) {
        throw new Error('Input must be an array of _id');
      }

      const activeModel = this.getModel(model);
      const result = await activeModel.deleteMany({ _id: { $in: ids } });
      console.log(`${result.deletedCount} items deleted.`);
      return result;
    } catch (error) {
      console.error('Error deleting items:', error);
      throw error;
    }
  }

  /**
   * Trova documenti secondo una query Mongoose.
   * @param {Object} [query={}] - Condizione di ricerca.
   * @param {Object} [options={}] - Opzioni (es. limit, sort).
   * @param {mongoose.Model|null} [model=null] - Modello alternativo.
   * @returns {Promise<Array>} Risultati della query.
   */
  async findItems(query = {}, options = {}, model = null) {
    try {
      const activeModel = this.getModel(model);
      const results = await activeModel.find(query, null, options);
      return results;
    } catch (error) {
      console.error('Error finding items:', error);
      throw error;
    }
  }

  /**
   * Esegue un'aggregazione MongoDB con pipeline.
   * @param {Array<Object>} pipeline - Fasi dell'aggregazione.
   * @param {mongoose.Model|null} [model=null] - Modello alternativo.
   * @returns {Promise<Array>} Risultati aggregati.
   */
  async runAggregation(pipeline, model = null) {
    try {
      const activeModel = this.getModel(model);
      const results = await activeModel.aggregate(pipeline);
      return results;
    } catch (error) {
      console.error('Error running aggregation:', error);
      throw error;
    }
  }

  /**
   * Chiude la connessione Mongoose con il database.
   */
  async disconnect() {
    try {
      await mongoose.disconnect();
      console.log('Disconnected from MongoDB');
    } catch (error) {
      console.error('Error disconnecting from MongoDB:', error);
      throw error;
    }
  }



  /**
     * Esegue una find "raw" su una collection senza schema.
     * @param {string} collectionName
     * @param {Object} [filter={}]
     * @param {Object} [options={}]  // es: { limit, skip, sort, projection, batchSize }
     * @returns {Promise<Array>}
     */
  async queryCollection(collectionName, filter = {}, options = {}) {
    try {
      const coll = mongoose.connection.db.collection(collectionName);
      const cursor = coll.find(filter, {
        limit: options.limit,
        skip: options.skip,
        sort: options.sort,
        projection: options.projection,
        batchSize: options.batchSize,
        readPreference: options.readPreference, // opzionale
      });
      const docs = await cursor.toArray();
      return docs;
    } catch (err) {
      console.error('Error in queryCollection:', err);
      throw err;
    }
  }

  /**
   * Esegue una aggregation "raw" su una collection senza schema.
   * @param {string} collectionName
   * @param {Array<Object>} pipeline
   * @param {Object} [options={}] // es: { allowDiskUse: true }
   * @returns {Promise<Array>}
   */
  async aggregateCollection(collectionName, pipeline, options = {}) {
    try {
      const coll = mongoose.connection.db.collection(collectionName);
      const cursor = coll.aggregate(pipeline, {
        allowDiskUse: options.allowDiskUse ?? true,
        maxTimeMS: options.maxTimeMS, // opzionale
        bypassDocumentValidation: options.bypassDocumentValidation,
      });
      const docs = await cursor.toArray();
      return docs;
    } catch (err) {
      console.error('Error in aggregateCollection:', err);
      throw err;
    }
  }



  /**
   * Aggiorna un campo (di default 'updatedAt') su tutti i documenti che rispettano il filtro.
   *
   * @param {Object} filter - Filtro per selezionare i documenti (es. { currentseason: '2025' }).
   * @param {string} [fieldName='updatedAt'] - Nome del campo da aggiornare.
   * @param {any} [value=new Date()] - Valore da assegnare al campo.
   * @param {mongoose.Model|null} [model=null] - Modello alternativo.
   * @returns {Promise<Object>} Risultato dell'updateMany.
   */
  async updateManyField(filter, fieldName = 'updatedAt', value = new Date(), model = null) {
    try {
      const activeModel = this.getModel(model);
      const result = await activeModel.updateMany(filter, { $set: { [fieldName]: value } });
      console.log(`${result.modifiedCount} documents updated (field: ${fieldName}).`);
      return result;
    } catch (error) {
      console.error('Error updating documents:', error);
      throw error;
    }
  }
  
  
}

module.exports = MongoPackage;