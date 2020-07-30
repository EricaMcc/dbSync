/**
 * API documentation
 * 
 * https://lodash.com/docs/4.17.15
 * https://www.npmjs.com/package/nedb
 * https://github.com/bajankristof/nedb-promises
 * https://www.npmjs.com/package/await-the#api-reference
 */

  const Datastore = require('nedb-promises');
  const _ = require('lodash');
  const the = require('await-the');
  
  // The source database to sync updates from
  let sourceDb = new Datastore({
    inMemoryOnly: true,
    timestampData: true
  });
  
  // The target database that sendEvents() will write too
  let targetDb = new Datastore({
    inMemoryOnly: true,
    timestampData: true
  });
  
  let TOTAL_RECORDS;
  
  const load = async () => {
    // Add some documents to the collection
    await sourceDb.insert({ name : 'GE', owner: 'test', amount: 1000000 });
    await the.wait(300);
    await sourceDb.insert({ name : 'Exxon', owner: 'test2', amount: 5000000 });
    await the.wait(300);
    await sourceDb.insert({ name : 'Google', owner: 'test3', amount: 5000001 });
  
    TOTAL_RECORDS = 3;
  }
  
  const reset = async () => {
    await sourceDb.remove({}, { multi: true });
    await targetDb.remove({}, { multi: true });
  }
  
  let EVENTS_SENT = 0;
  /**
   * Api to send each document to in order to sync
   */
  const sendEvent = async (data) => {
    EVENTS_SENT += 1;
    console.log('event being sent: ', data);
    
    //todo - bonus: write data to targetDb
    await targetDb.insert(data);
  };
  
  // Find and update an existing document
  const touch = async (name) => {
      await sourceDb.update({ name }, { $set: { owner: 'test4' } });
  };
  
  const dump = async name => {
    const record = await sourceDb.findOne({ name });
    // console.log(record);
  };
  
  /**
   * Get all records out of the database and send them using
   * 'sendEvent()'
   */
  const syncAllNoLimit = async () => {
      const allRecords = await sourceDb.find();
      await sendEvent(allRecords);
  };
  
  /**
   * Sync up to the provided limit of records. Data returned from
   * this function will be provided on the next call as the data 
   * argument
   */
  
  const syncWithLimit = async (limit, data) => {
    // using pagination to send data in batches to targetDb
    const records = await sourceDb.find({}).skip(data.count * limit).limit(limit).exec();
    await sendEvent(records);
    data.results = data.results.concat(records);
    return data;
  }
  
  /**
   * Synchronize in given batch sizes.  This is needed to get around 
   * limits most APIs have on result sizes.
   */
  
  const syncAllSafely = async (batchSize, data) => {
  
    if (_.isNil(data)) {
      data = {};
    }
    data.count = 0;
    data.results = [];
    let numOfRecords = await sourceDb.count({});
    await the.while(
      () => data.count < numOfRecords, 
      async () => {
        data = await syncWithLimit(batchSize, data);
        data.count++;
        // console.log('DATA', data);
        return data;
      }
    );
    
    return data;
  }
  
  
  
  /**
   * Sync changes since the last time the function was called
   * with the passed in data
   */
  const syncNewChanges = async (limit, data) => {
    let count = 0;
    const { results } = data;
    // console.log('polling');
  
    const newData = await the.while(
      () => count < data.count, 
      async () => {
        const { _id } = results[count];
        const [sourceRecord] = await sourceDb.find({ _id });
        if (sourceRecord.updatedAt !== results[count].updatedAt) {
          // console.log('something changed');
          // Would probably never do this in real life, but soft-delete instead
          // Deleting record and calling send event again to make sure that timestamps match
          await targetDb.remove({ _id });
          await sendEvent(sourceRecord);
          results[count] = sourceRecord;
        } else {
          // this was just for me to confirm
          // console.log('no changes');
        }
        count++;
        return data;
      }
    );
    // console.log('NEW DATA', data);
  
    if (limit > 0) {
      await the.wait(5000);
      await syncNewChanges(limit - 1, newData);
    } else {
      return newData;
    }
  
  }
  
  /**
   * Implement function to fully sync of the database and then 
   * keep polling for changes.
   */
  const synchronize = async () => {
    const data = await syncAllSafely(1);
    await the.wait(5000);
    // passing it 10 so it only checks for updates 10 times
    await syncNewChanges(10, data);
  }
  

  
  const runTest = async () => {
    await load();
  
    await dump('GE');
  
    EVENTS_SENT = 0;
    await syncAllNoLimit();
  
    if (EVENTS_SENT === TOTAL_RECORDS) {
      console.log('1. synchronized correct number of events')
    }
    
    // resetting db
    await reset();
    console.log('DB', await sourceDb.find({}).exec());
    console.log('DB', await targetDb.find({}).exec());
    await load();
  
    EVENTS_SENT = 0;
    let data = await syncAllSafely(2);
  
    if (EVENTS_SENT === TOTAL_RECORDS) {
      console.log('2. synchronized correct number of events')
    }
  
    // Makes some updates and then sync just the changed files
    EVENTS_SENT = 0;
    await the.wait(300);
    await touch('GE');
    await syncNewChanges(1, data);
  
    if (EVENTS_SENT === 1) {
      console.log('3. synchronized correct number of events')
    }
  
  
  }

runTest();