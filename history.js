/**
 *  Marcos H. Trotti 2016
 *  This script store detailed information about app status
 * 
 **/
 
 var gplay = require('google-play-scraper');
 var when = require('when');
 var mysql = require('mysql');

 var conn = mysql.createConnection({
    host     : 'localhost',
    user     : 'scraper',
    password : '',
    database : 'scraper'
 });
 
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
                    console.log('[Erroor] '+ err);
                resolve(true);
                console.log('++ + + + + Retrieved application: + + + + ',escapedAppId);
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
                console.log('Resolved retrieved application: ',appid);
                resolve(true);
            });
        })
        .catch(function(e){
            console.log('There was an error fetching the application [',  appid,'] ', e.message);
            resolve(true);
        }); 
    });
    return promise;   
}

// Search all categories and collections
conn.connect();
var deferreds = [];

function getAppsFrom(offset){
    /**
     * Import 30 apps per batch to prevent socket hang up
     */
    var appsQuery='SELECT appId FROM apps LIMIT 30 OFFSET '+offset;
    conn.query(appsQuery, function(err, rows, fields) {
        if (err) 
            console.log('[Err] '+ err);
        else{
            deferreds = [];
            rows.forEach(function(entry){
                deferreds.push(getAppInformation(entry.appId));     
            });
            when.all(deferreds).then(function () {
                if (deferreds.length>0)
                    getAppsFrom(offset+30);
                else{
                    console.log('++ All apps had been searched');
                    conn.end();
                }
                console.log('++ +++ All Promises were finished');
            });
        }
    });  
}

/**
 * Start process
 */
getAppsFrom(0);