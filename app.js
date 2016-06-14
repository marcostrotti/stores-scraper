/**
 *  Marcos H. Trotti 2016
 *  
 **/
 
 var gplay = require('google-play-scraper');
 var when = require('when');
 var mysql = require('mysql');
 var winston = require('winston');
 var logger = new winston.Logger({
    level: 'verbose',
    transports: [
      new (require('winston-daily-rotate-file'))({
      colorize: 'false',
      json: false,
      filename: 'log/scraper-apps.log',
      datePattern: '.yyyy-MM-dd'
    })
    ]
  });

 var conn;
 

 
function getAppInformation(appEntry){
    
    gplay.app({appId: appEntry.appId})
    .then(function(app){
        logger.info('Retrieved application: ' + app.appId);
    })
    .catch(function(e){
        logger.error('There was an error fetching the application!');
    }); 
}

function updateOrInsertApp(appEntry){
    var escapedAppId=conn.escape(appEntry.appId);
    var escapedTitle=conn.escape(appEntry.title);
    var escapedDeveloper=conn.escape(appEntry.developer);
    var strQuery= "INSERT INTO apps (appId, title, developer) VALUES ("+escapedAppId+", "+escapedTitle+", "+escapedDeveloper+") " + 
    " ON DUPLICATE KEY UPDATE title="+escapedTitle+" , developer="+escapedDeveloper;
    return conn.query(strQuery, function(err, rows, fields) {
        if (err) 
            logger.error('[Erroor] '+ err);
    });  
    
}


function scrapCategory(aCategory,aCollection){
    var promise = when.promise(function(resolve, reject, notify) {
        gplay.list({
            category: aCategory,
            collection: aCollection,
            num: 120
        })
        .then(function(apps){
            var defs = [];
            apps.forEach(function(appEntry) {    
                defs.push(updateOrInsertApp(appEntry));
            });
            when.all(defs).then(function () {
                logger.info('Finished Promises ',aCategory,' - ',aCollection);
                resolve(true);
            });
            logger.info('Resolve ',aCategory,' - ',aCollection);
        })
        .catch(function(e){
            logger.error('There was an error fetching the list! ',e.message);
            resolve(false);
        });
    });
    return promise;
}

// Search all categories and collections
function handleDisconnect() {
  conn = mysql.createConnection({
    host     : 'localhost',
    user     : 'scraper',
    password : '',
    database : 'scraper',
    charset : 'utf8mb4'
    });

  conn.connect(function(err) {              // The server is either down
    if(err) {                                     // or restarting (takes a while sometimes).
      logger.error('error when connecting to db:', err);
      setTimeout(handleDisconnect, 1000); // We introduce a delay before attempting to reconnect,
    }                                     // to avoid a hot loop, and to allow our node script to
  });                                     // process asynchronous requests in the meantime.
                                          // If you're also serving http, display a 503 error.
  conn.on('error', function(err) {
    logger.error('db error', err);
    if(err.code === 'PROTOCOL_CONNECTION_LOST') { // Connection to the MySQL server is usually
      handleDisconnect();                         // lost due to either server restart, or a
    } else {                                      // connnection idle timeout (the wait_timeout
      throw err;                                  // server variable configures this)
    }
  });
}

handleDisconnect(); 
var pendingFunctions=[];
/**
 * TODO: improve to provent socker hang up when query
 */
for (var i in gplay.category) {
    for (var j in gplay.collection){
        pendingFunctions.push(scrapCategory(gplay.category[i],gplay.collection[j]));
    }
}

when.all(pendingFunctions).then(function () {
        conn.end();
        logger.info('++ +++ All Promises were finished');       
});