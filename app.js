/**
 *  
 **/
 
 var gplay = require('google-play-scraper');
 var when = require('when');
 
 var mysql      = require('mysql');
 
 var connection = mysql.createConnection({
    host     : 'localhost',
    user     : 'scraper',
    password : '',
    database : 'scraper'
 });

 
function getAppInformation(appEntry){
    
    gplay.app({appId: appEntry.appId})
    .then(function(app){
        console.log('Retrieved application: ' + app.appId);
    })
    .catch(function(e){
        console.log('There was an error fetching the application!');
    }); 
}

function updateOrInsertApp(appEntry){
    var escapedAppId=connection.escape(appEntry.appId);
    var escapedTitle=connection.escape(appEntry.title);
    var escapedDeveloper=connection.escape(appEntry.developer);
    var strQuery= "INSERT INTO apps (appId, title, developer) VALUES ("+escapedAppId+", "+escapedTitle+", "+escapedDeveloper+") " + 
    " ON DUPLICATE KEY UPDATE title="+escapedTitle+" , developer="+escapedDeveloper;
    connection.query(strQuery, function(err, rows, fields) {
        if (err) 
            console.log('[Erroor] '+ err);
    });  
    
}


function scrapCategory(aCategory,aCollection){
    gplay.list({
        category: aCategory,
        collection: aCollection,
        num: 120
    })
    .then(function(apps){
        
        apps.forEach(function(appEntry) {    
            updateOrInsertApp(appEntry);
        });
       // connection.end(function(err) { if(err) console.error('Error On DB Close.'); });
    })
    .catch(function(e){
        console.log('There was an error fetching the list!');
    });
}


// Search all categories and collections
connection.connect();
var deferreds = [];
//scrapCategory(gplay.category.ANDROID_WEAR,gplay.collection.TOP_FREE);
for (var i in gplay.category) {
    for (var j in gplay.collection){
        console.log("Scraping ", gplay.category[i]," collection ", gplay.collection[j]);
        deferreds.push(scrapCategory(gplay.category[i],gplay.collection[j]));
        //scrapCategory(gplay.category[i],gplay.collection[j]);
    }
}

when.all(deferreds).then(function () {
    console.log('Finished Promises');
});