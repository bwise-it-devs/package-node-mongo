const mongoose = require('mongoose');

class MongoPackage {
  constructor(settings) {
    if (!settings.mongoUri) {
      throw new Error("Missing 'mongoUri' in settings");
    }
    if (!settings.model) {
      throw new Error("Missing 'model' in settings");
    }

    this.mongoUri = settings.mongoUri;
    this.model = settings.model;

    this.connect();
  }

  async connect() {
    try {
      await mongoose.connect(this.mongoUri, {
        useNewUrlParser: true,
        useUnifiedTopology: true,
      });
      console.log('Connected to MongoDB');
    } catch (error) {
      console.error('Error connecting to MongoDB:', error);
      throw error;
    }
  }

  async insertItem(item, updateIfExists = false) {
    try {
      if (updateIfExists) {
        // Upsert: inserisce o aggiorna se esiste
        const result = await this.model.findByIdAndUpdate(item._id, item, {
          new: true,
          upsert: true,
        });
        return result;
      } else {
        const newItem = new this.model(item);
        await newItem.save();
        return newItem;
      }
    } catch (error) {
      console.error('Error inserting item:', error);
      throw error;
    }
  }

  async insertArray(items, updateIfExists = false) {
    try {
      const results = [];
      for (const item of items) {
        const result = await this.insertItem(item, updateIfExists);
        results.push(result);
      }
      return results;
    } catch (error) {
      console.error('Error inserting array:', error);
      throw error;
    }
  }


  async deleteItems(ids) {
    try {
      if (!Array.isArray(ids)) {
        throw new Error('Input must be an array of _id');
      }

      const result = await this.model.deleteMany({ _id: { $in: ids } });
      console.log(`${result.deletedCount} items deleted.`);
      return result;
    } catch (error) {
      console.error('Error deleting items:', error);
      throw error;
    }
  }
   
}

module.exports = MongoPackage;
