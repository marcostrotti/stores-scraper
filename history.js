/**
 *  Marcos H. Trotti 2016
 *  This script store detailed information about app status
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
      filename: 'log/scraper-history.log',
      datePattern: '.yyyy-MM-dd'
    })
    ]
  });
  
 var conn;
 
function insertStatus(appEntry){
    var escapedAppId=conn.escape(appEntry.appId);
    var escapedTitle=conn.escape(appEntry.title);
    var escapedUrl=conn.escape(appEntry.url);
    var escapedIcon=conn.escape(appEntry.icon);
    var escapedHistogram=conn.escape(appEntry.histogram);
    var escapedDescription=conn.escape(appEntry.description);
    var escapedDescriptionHTML=conn.escape(appEntry.descriptionHTML);
    var escapedDeveloper=conn.escape(appEntry.developer);
    var escapedDeveloperEmail=conn.escape(appEntry.developerEmail);
    var escapedDeveloperWebSite=conn.escape(appEntry.developerWebsite);
    var escapedGenre=conn.escape(appEntry.genre);
    var escapedGenreId=conn.escape(appEntry.genreId);
    var escapedFamilyGenre=conn.escape(appEntry.familyGenre);
    var escapedfamilyGenreId=conn.escape(appEntry.familyGenreId);
    var escapedSize=conn.escape(appEntry.size);
    var escapedRequiredAndroidVersion=conn.escape(appEntry.requiredAndroidVersion);
    var escapedContentRating=conn.escape(appEntry.contentRating);
    var escapedScreenshots=conn.escape(appEntry.screenshots);
    var escapedVideo=conn.escape(appEntry.video);
    var escapedComments=conn.escape(appEntry.comments);
    
    var strQuery= "INSERT INTO stats SET " +
         "appId="+ escapedAppId +" , "+
         "title="+escapedTitle +", "+
         "url="+escapedUrl +", "+
         "icon="+escapedIcon +", "+
         "minInstalls="+appEntry.minInstalls +", "+
         "maxInstalls="+appEntry.maxInstalls +", "+
         "score="+conn.escape(appEntry.score) +", "+
         //"reviews="+appEntry.reviews +" , "+
         //"histogram="+escapedHistogram +" , "+
         "description="+escapedDescription +", "+
         "descriptionHTML="+escapedDescriptionHTML +", "+
         "developer="+escapedDeveloper +", "+
         "developerEmail="+escapedDeveloperEmail +", "+
         "developerWebsite="+escapedDeveloperWebSite +", "+
         "updated="+conn.escape(appEntry.updated) +", "+
         "genre="+escapedGenre +", "+
         "genreId="+escapedGenreId +", "+
         "familyGenre="+escapedFamilyGenre +", "+
         "familyGenreId="+escapedfamilyGenreId +", "+
         "version="+conn.escape(appEntry.version) +", "+
         "size="+escapedSize +", "+
         "requiredAndroidVersion="+escapedRequiredAndroidVersion +", "+
         "contentRating="+escapedContentRating +", "+
         "price="+conn.escape(appEntry.price) +", "+
         "free="+appEntry.free +", "+
         "video="+escapedVideo;
    /**
     * 
     * Ever return true whitout import insert result
     *  */          
    return when.promise(function(resolve, reject, notify) {
            conn.query(strQuery, function(err, rows, fields) {
                if (err) 
                    logger.error('[Erroor] '+ err);
                resolve(true);
                logger.info('++ + + + + Retrieved application: + + + + ',escapedAppId);
            });
        });    
    
}

function getAppInformation(appid){
    var promise = when.promise(function(resolve, reject, notify) {
        gplay.app({appId: appid})
        .then(function(app){
            var defs=[];
            defs.push(insertStatus(app));
            when.all(defs).then(function () {
                logger.error('Resolved retrieved application: ',appid);
                resolve(true);
            });
        })
        .catch(function(e){
            logger.info('There was an error fetching the application [',  appid,'] ', e.message);
            resolve(true);
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
    charset  : 'latin1'
    });

  conn.connect(function(err) {              // The server is either down
    if(err) {                                     // or restarting (takes a while sometimes).
      logger.error('error when connecting to db:', err);
      setTimeout(handleDisconnect, 1000); // We introduce a delay before attempting to reconnect,
    }                                     // to avoid a hot loop, and to allow our node script to
  });                                     // process asynchronous requests in the meantime.
                                          // If you're also serving http, display a 503 error.
  conn.on('error', function(err) {
    logger.info('db error', err);
    if(err.code === 'PROTOCOL_CONNECTION_LOST') { // Connection to the MySQL server is usually
      handleDisconnect();                         // lost due to either server restart, or a
    } else {                                      // connnection idle timeout (the wait_timeout
      throw err;                                  // server variable configures this)
    }
  });
}

handleDisconnect(); 

var deferreds = [];

function getAppsFrom(offset){
    /**
     * Import 30 apps per batch to prevent socket hang up
     */
    var appsQuery='SELECT appId FROM apps LIMIT 30 OFFSET '+offset;
    conn.query(appsQuery, function(err, rows, fields) {
        if (err) 
            logger.error('[Err] '+ err);
        else{
            deferreds = [];
            rows.forEach(function(entry){
                deferreds.push(getAppInformation(entry.appId));     
            });
            when.all(deferreds).then(function () {
                if (deferreds.length>0)
                    getAppsFrom(offset+30);
                else{
                    logger.info('++ All apps had been searched');
                    conn.end();
                }
                logger.info('++ +++ All Promises were finished');
            });
        }
    });  
}

/**
 * Start process
 */
getAppsFrom(0);