/**
 * MongoPackage
 * ------------
 * Libreria unica per gestire connessioni MongoDB tramite Mongoose e MongoClient nativo.
 * 
 * Include:
 * - Connessione Mongoose (schema-based)
 * - Connessione MongoClient (raw driver)
 * - Singleton globale con reconnect automatico
 * - Supporto logging e prevenzione connessioni parallele
 * - Metodi CRUD, aggregazioni e query schema-less
 * 
 * Retrocompatibile con il vecchio `package-node-mongo` e `mongoSingleton.js`
 */

const mongoose = require('mongoose');
const { MongoClient } = require('mongodb');

class MongoPackage {
  // --- Proprietà statiche per il singleton ---
  static instance = null;
  static isConnecting = false;

  /**
   * Restituisce un'istanza singleton condivisa (retrocompatibile)
   * @param {Object} settings - { mongoUri, model, dbName? }
   * @param {Object} logger - opzionale, default console
   */
  static async getInstance(settings, logger = console) {
    if (this.instance && this.instance.isConnected()) {
      return this.instance;
    }

    if (this.isConnecting) {
      logger.warn('⚠️ MongoPackage: connessione in corso, attendo...');
      await new Promise((resolve) => setTimeout(resolve, 500));
      return this.instance;
    }

    this.isConnecting = true;

    try {
      const pkg = new MongoPackage(settings, logger);
      await pkg.connect();

      this.instance = pkg;
      this.isConnecting = false;

      logger.info(`✅ MongoPackage connesso a ${settings.mongoUri}`);
      return this.instance;
    } catch (err) {
      this.isConnecting = false;
      logger.error('❌ Errore connessione MongoPackage:', err);
      throw err;
    }
  }

  /**
   * Costruttore: inizializza ma non connette subito
   */
  constructor(settings, logger = console) {
    if (!settings?.mongoUri) {
      throw new Error("Missing 'mongoUri' in settings");
    }

    this.mongoUri = settings.mongoUri;
    this.defaultModel = settings.model || null;
    this.dbName = settings.dbName || null;
    this.logger = logger;

    this.client = null;
    this.db = null;
  }

  /**
   * Controlla se il client MongoDB nativo è connesso
   */
  isConnected() {
    return (
      this.client?.topology?.isConnected?.() ||
      mongoose.connection.readyState === 1
    );
  }

  /**
   * Connette sia Mongoose che il driver nativo MongoClient
   */
  async connect() {
    try {
      // ✅ Connessione Mongoose
      await mongoose.connect(this.mongoUri);
      this.logger.info('🧩 Mongoose connesso');

      // ✅ Connessione MongoClient nativo
      this.client = new MongoClient(this.mongoUri, {
        serverSelectionTimeoutMS: 10000,
        maxPoolSize: 20,
      });
      await this.client.connect();

      // --- Determina il nome del database ---
      let dbName = this.dbName;
      if (!dbName) {
        const match = this.mongoUri.match(/\/([^/?]+)(\?|$)/);
        dbName = match ? match[1] : 'test';
      }

      this.db = this.client.db(dbName);
      this.logger.info(`🧠 MongoClient connesso al database: ${dbName}`);

      // --- Eventi ---
      this.client.on('close', async () => {
        this.logger.warn('⚠️ MongoDB connection closed. Tentativo di riconnessione...');
        MongoPackage.instance = null;
        try {
          await MongoPackage.getInstance({ mongoUri: this.mongoUri, dbName: this.dbName }, this.logger);
        } catch (err) {
          this.logger.error('❌ Riconnessione fallita:', err);
        }
      });

      this.client.on('error', (err) => {
        this.logger.error('❌ Errore MongoDB:', err);
      });

      this.client.on('reconnect', () => {
        this.logger.info('🔄 MongoDB riconnesso correttamente');
      });

    } catch (error) {
      this.logger.error('❌ Errore connessione:', error);
      throw error;
    }
  }

  /**
   * Chiude tutte le connessioni
   */
  async disconnect() {
    try {
      if (this.client) {
        await this.client.close();
        this.logger.info('🛑 MongoClient chiuso');
      }
      await mongoose.disconnect();
      this.logger.info('🛑 Mongoose disconnesso');
    } catch (error) {
      this.logger.error('❌ Errore disconnessione:', error);
      throw error;
    }
  }

  // ------------------------------------------------------------------
  // 🔽 Tutti i metodi CRUD e aggregazione originali (retrocompatibili)
  // ------------------------------------------------------------------

  /**
   * Helper: crea/riusa un modello Mongoose "schema-less" per una collection
   */
  getSchemaLessModel(collectionName) {
    const { Schema } = mongoose;
    const schema = new Schema({}, {
      strict: false,
      collection: collectionName,
      timestamps: false,
    });
    const modelName = `SchemaLess_${collectionName}`;
    return mongoose.models[modelName] || mongoose.model(modelName, schema);
  }

  /**
   * Restituisce il modello da usare (predefinito o passato al metodo).
   */
  getModel(model) {
    if (!model && !this.defaultModel) {
      throw new Error("No model specified or default model provided");
    }
    return model || this.defaultModel;
  }

  /**
   * Inserisce un singolo documento, con possibilità di upsert.
   */
  async insertItem(item, updateIfExists = false, model = null) {
    try {
      const activeModel = this.getModel(model);

      if (updateIfExists) {
        const result = await activeModel.findByIdAndUpdate(
          item._id,
          { $set: item },
          { new: true, upsert: true }
        );
        return result;
      } else {
        const newItem = new activeModel(item);
        await newItem.save();
        return newItem;
      }
    } catch (error) {
      this.logger.error('Error inserting item:', error);
      throw error;
    }
  }

  /**
   * Verifica se esiste almeno un documento che soddisfa la query.
   */
  async existsItem(query, model = null) {
    try {
      const activeModel = this.getModel(model);
      const exists = await activeModel.exists(query);
      return !!exists;
    } catch (error) {
      this.logger.error('Error checking item existence:', error);
      throw error;
    }
  }

  /**
   * Inserisce un array di documenti, con possibilità di upsert.
   */
  async insertArray(items, updateIfExists = false, model = null) {
    try {
      if (!items || items.length === 0) {
        this.logger.info('insertArray.items empty');
        return [];
      }

      const results = [];
      for (const item of items) {
        const result = await this.insertItem(item, updateIfExists, model);
        results.push(result);
      }
      return results;
    } catch (error) {
      this.logger.error('Error inserting array:', error);
      throw error;
    }
  }

  /**
   * Cancella documenti dato un array di `_id`.
   */
  async deleteItems(ids, model = null) {
    try {
      if (!Array.isArray(ids)) {
        throw new Error('Input must be an array of _id');
      }

      const activeModel = this.getModel(model);
      const result = await activeModel.deleteMany({ _id: { $in: ids } });
      this.logger.info(`${result.deletedCount} items deleted.`);
      return result;
    } catch (error) {
      this.logger.error('Error deleting items:', error);
      throw error;
    }
  }

  /**
   * Trova documenti secondo una query Mongoose o in schema-less passando il nome collection.
   */
  async findItems(query = {}, options = {}, model = null) {
    try {
      if (typeof options === 'string' && !model) {
        model = this.getSchemaLessModel(options);
        options = {};
      }
      if (typeof model === 'string' || model instanceof String) {
        model = this.getSchemaLessModel(model);
      }

      const activeModel = this.getModel(model);
      const { projection = null, ...opts } = options || {};
      const results = await activeModel.find(query, projection, opts);
      return results;
    } catch (error) {
      this.logger.error('Error finding items:', error);
      throw error;
    }
  }

  /**
   * Esegue un'aggregazione con Mongoose o schema-less passando il nome collection.
   */
  async runAggregation(pipeline, model = null, options = {}) {
    try {
      if (typeof model === 'string' || model instanceof String) {
        model = this.getSchemaLessModel(model);
      }

      const activeModel = this.getModel(model);
      const agg = activeModel.aggregate(pipeline, {
        allowDiskUse: options.allowDiskUse ?? true,
      });

      if (options.maxTimeMS) {
        agg.option({ maxTimeMS: options.maxTimeMS });
      }

      const results = await agg.exec();
      return results;
    } catch (error) {
      this.logger.error('Error running aggregation:', error);
      throw error;
    }
  }

  /**
   * Esegue una find "raw" su una collection senza schema.
   */
  async queryCollection(collectionName, filter = {}, options = {}) {
    try {
      const coll = this.db.collection(collectionName);
      const cursor = coll.find(filter, {
        limit: options.limit,
        skip: options.skip,
        sort: options.sort,
        projection: options.projection,
        batchSize: options.batchSize,
        readPreference: options.readPreference,
      });
      const docs = await cursor.toArray();
      return docs;
    } catch (err) {
      this.logger.error('Error in queryCollection:', err);
      throw err;
    }
  }

  /**
   * Esegue una aggregation "raw" su una collection senza schema.
   */
  async aggregateCollection(collectionName, pipeline, options = {}) {
    try {
      const coll = this.db.collection(collectionName);
      const cursor = coll.aggregate(pipeline, {
        allowDiskUse: options.allowDiskUse ?? true,
        maxTimeMS: options.maxTimeMS,
        bypassDocumentValidation: options.bypassDocumentValidation,
      });
      const docs = await cursor.toArray();
      return docs;
    } catch (err) {
      this.logger.error('Error in aggregateCollection:', err);
      throw err;
    }
  }

  /**
   * Aggiorna un campo (di default 'updatedAt') su tutti i documenti che rispettano il filtro.
   */
  async updateManyField(filter, fieldName = 'updatedAt', value = new Date(), model = null) {
    try {
      const activeModel = this.getModel(model);
      const result = await activeModel.updateMany(filter, { $set: { [fieldName]: value } });
      this.logger.info(`${result.modifiedCount} documents updated (field: ${fieldName}).`);
      return result;
    } catch (error) {
      this.logger.error('Error updating documents:', error);
      throw error;
    }
  }
}

module.exports = MongoPackage;
