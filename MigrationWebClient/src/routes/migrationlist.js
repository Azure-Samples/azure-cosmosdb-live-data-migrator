var DocumentDBClient = require('documentdb').DocumentClient;
var async = require('async');
var state = require('./state')

function MigrationList(migrationDao) {
  this.migrationDao = migrationDao;
}

MigrationList.prototype = {
  showMigrations: function(req, res) {
    var self = this;
    setTimeout(function() {
      try {
        var querySpec = {
          query: 'SELECT * FROM root r where NOT r.completed'
        };

        self.migrationDao.find(querySpec, function(err, items) {
          if (err) {
            res.render('error', {
              title: 'Migration',
              error: err
            });

            return;
          }
      
          if(items === undefined || items.length === 0) {
            res.render('index', {
              title: 'Migration',
              migration: undefined
            });
          } else {
          
              var migration = items[0];

              if (migration.startTime !== undefined) {
                migration.elapsedTime = (Date.now() - migration.startTime) * 1.0 / ( 1000 * 3600 );
              } else {
                migration.elapsedTime = 0
              }

              migration.percentageCompleted = migration.percentageCompleted || 0
              migration.sourceCollectionCount = migration.sourceCollectionCount || 0
              migration.destinationCollectionCount = migration.destinationCollectionCount || 0
              migration.eta = migration.eta || 0
              migration.averageInsertRate = migration.averageInsertRate || 0
              migration.currentInsertRate = migration.currentInsertRate || 0
            
              state.migration = migration;

              res.render('index', {
                title: 'Migration',
                migration: migration
              });
          }
        });
      } catch(e) {
        res.render('error', {
          title: 'Migration',
          error: e
        });

        return;
      }
    }, 1500, 'funky');
  },

  addMigration: function(req, res) {
    var self = this;
    var item = req.body;
    item.startTime = Date.now();
    item.leaseThroughput = "10000";
    self.migrationDao.addItem(item, function(err) {
      if (err) {
        res.render('error', {
          title: 'Migration',
          error: err
        });

        return;
      }

      res.redirect('/');
    });
  },

  completeMigration: function(req, res) {
    var self = this;
    state.migration.completed = true;
    self.migrationDao.updateItem(state.migration, function(err, replaced) {
      state.migration = null;
      state.srcClient = null;
      state.destClient = null;
      if (err) {
        throw err;
      } else {
        res.redirect('/');
      }
    });
  },

  reverseMigration: function(req, res) {
    var self = this;
    state.migration.completed = true;
    self.migrationDao.updateItem(state.migration, function(err, replaced) {
      if (err) {
        res.render('error', {
          title: 'Migration',
          error: err
        });

        return;
      } else {
        
        var migration = state.migration;

        var reverseMigration = {
          "monitoredUri": migration.destUri,
          "monitoredSecretKey": migration.destSecretKey,
          "monitoredDbName": migration.destDbName,
          "monitoredCollectionName": migration.destCollectionName, 
          "destUri": migration.monitoredUri,
          "destSecretKey": migration.monitoredSecretKey,
          "destDbName": migration.monitoredDbName,
          "destCollectionName": migration.monitoredCollectionName,
          "monitoredThroughput": 0,
          "leaseThroughput": 10000,
          "leaseUri": migration.leaseUri,
          "leaseSecretKey": migration.leaseSecretKey,
          "leaseDbName": migration.leaseDbName,
          "leaseCollectionName": migration.leaseCollectionName + Date.now(),
          "dataAgeInHours": -1
        }

        state.migration = null;
        state.srcClient = null;
        state.destClient = null;   
        
        self.addMigration({body: reverseMigration}, res)
      }
    });
  }
};

module.exports = MigrationList;
