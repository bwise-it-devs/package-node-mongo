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
 * Note:
 * -----
 * - È possibile passare un model alternativo ad ogni metodo, utile se si lavora con più collezioni.
 * - In caso di errore, i metodi loggano l’errore e lo rilanciano.
 */


const mongoose = require('mongoose');

class MongoPackage {
  constructor(settings) {
    if (!settings.mongoUri) {
      throw new Error("Missing 'mongoUri' in settings");
    }

    this.mongoUri = settings.mongoUri;
    this.defaultModel = settings.model || null;

    this.connect();
  }

  async connect() {
    try {
      await mongoose.connect(this.mongoUri);
      console.log('Connected to MongoDB');
    } catch (error) {
      console.error('Error connecting to MongoDB:', error);
      throw error;
    }
  }

  getModel(model) {
    if (!model && !this.defaultModel) {
      throw new Error("No model specified or default model provided");
    }
    return model || this.defaultModel;
  }

  async insertItem(item, updateIfExists = false, model = null) {
    try {
      const activeModel = this.getModel(model);
      if (updateIfExists) {
        // Upsert: inserisce o aggiorna se esiste
        const result = await activeModel.findByIdAndUpdate(item._id, item, {
          new: true,
          upsert: true,
        });
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

  async existsItem(query, model = null) {
    try {
      const activeModel = this.getModel(model);
      const exists = await activeModel.exists(query); // Usa `exists` per una verifica veloce
      return !!exists; // Ritorna `true` se esiste, `false` altrimenti
    } catch (error) {
      console.error('Error checking item existence:', error);
      throw error;
    }
  }


  async insertArray(items, updateIfExists = false, model = null) {
    try {
      // Controlla se l'array è vuoto o nullo
      if (!items || items.length === 0) {
        console.log('insertArray.items empty');
        return []; // Restituisce un array vuoto immediatamente
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
}

module.exports = MongoPackage;
