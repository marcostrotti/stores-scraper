/**
 *  
 **/
 
 var gplay = require('google-play-scraper');
 
 
function scrapCategory(aCategory,aCollection){
    gplay.list({
        category: aCategory,
        collection: aCollection,
        num: 120
    })
    .then(function(apps){
        console.log('Retrieved ' + apps.length + ' applications!');
        apps.forEach(function(appEntry) {
            gplay.app({appId: appEntry.appId})
                .then(function(app){
                console.log('Retrieved application: ' + app.appId);
            })
            .catch(function(e){
                console.log('There was an error fetching the application!');
            });
        });
    })
    .catch(function(e){
        console.log('There was an error fetching the list!');
    });
}


scrapCategory(gplay.category.GAME_ACTION,gplay.collection.TOP_FREE);