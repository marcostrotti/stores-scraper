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
    return connection.query(strQuery, function(err, rows, fields) {
        if (err) 
            console.log('[Erroor] '+ err);
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
                console.log('Finished Promises ',aCategory,' - ',aCollection);
                resolve(true);
            });
            console.log('Resolve ',aCategory,' - ',aCollection);
        })
        .catch(function(e){
            console.log('There was an error fetching the list! ',e.message);
            resolve(false);
            //resolve(false);
        });
    });
    return promise;
}


// Search all categories and collections
connection.connect();
var deferreds = [];

for (var i in gplay.category) {
    for (var j in gplay.collection){
        deferreds.push(scrapCategory(gplay.category[i],gplay.collection[j]));
    }
}

when.all(deferreds).then(function () {
    connection.end();
    console.log('++ +++ All Promises were finished');
});