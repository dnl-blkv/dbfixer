// A simple example iterating over a query using the each function of the cursor.
var MongoClient = require('mongodb').MongoClient,
	test = require('assert');

// Counters
var num = 0;
var numFail = 0;

MongoClient.connect('mongodb://localhost:27017/test', function(err, db) {

	// Open a collection
	var nestedAlcdefCollection = db.collection('alcdef_n');
	var propertiesCollection = db.collection('adata');
	var mergedAlcdefCollection = db.collection('alcdef_p');
	
	nestedAlcdefCollection.distinct("metadata.objectnumber", function (err, uniqueNumbers) {
		propertiesCollection.find({"number" : {"$in": uniqueNumbers}}).toArray(function (err, propertiesArray) {
			var propertiesMap = buildPropertiesMap(propertiesArray);
			
			// Grab a cursor
			var cursor = nestedAlcdefCollection.find({});
			
			// Execute the each command, triggers for each document
			cursor.each(function(err, item) {

				// If the item is null then the cursor is exhausted/empty and closed
				if(item == null) {
					// Show that the cursor is closed
					cursor.toArray(function(err, items) {
						test.equal(null, err);

						// Close the db
						db.close();
					});
				} else {
					
					var data = item['data'];
					var newData;
					var dataCount = data.length;
					var metadata = item['metadata'];
					var objectnumber = metadata['objectnumber'];
					
					if ((dataCount >= 26) && (objectnumber)) {
						if (dataCount < 36) {
							newData = addMissingData(data);
						} else if (dataCount == 36) {
							newData = data;
						} else if (36 < dataCount){
							newData = dropExtraData(data);
						}
						
						var plainData = plainifyData(newData);

						// Get emetadata
						var emetadata = propertiesMap[objectnumber];
						
						if (emetadata) {
							stickAndInsert(mergedAlcdefCollection, emetadata, metadata, plainData);
							num ++;
							console.log('SUCCESS: ', num);
						} else {
							numFail ++;
							console.log('FAIL: ', numFail);
						}
					}
				}
			});
		});
	}); 
});

function buildPropertiesMap(propertiesArray) {
	var propertiesMap = {};
	
	for (var i = 0; i < propertiesArray.length; i ++) {
		propertiesMap[propertiesArray[i]['number'] + ''] = propertiesArray[i];
	}
	
	return propertiesMap;
}

function stickAndInsert (collection, emetadata, metadata, plainData) {
	var merged = {};
	
	stick(merged, emetadata);
	stick(merged, metadata);
	stick(merged, plainData);
	
	delete(merged['_id']);
	
	// INSERTING :) :)
	
	collection.insert(merged, function (err, result) {
		console.log('INSERTED');
	});
}

function stick (to, from) {
	var fromKeys = Object.keys(from);
	
	for (var i = 0; i < fromKeys.length; i ++) {
		var key = fromKeys[i];
		
		to[key] = from[key];
	}
}

function dropExtraData (data) {
	var newData = [];
	var requiredSize = 36;
	
	var dataCount = data.length;
	
	for (var i = 0; i < requiredSize; i ++) {
		newData[i] = data[Math.round((i / requiredSize) * dataCount)];
	}
	
	return newData;
}

function addMissingData (data) {
	var newData = [];
	var requiredSize = 36;
	var oldIndex = -1;
	var indicesToUpdate = [];
	var dataCount = data.length;
	
	newData[0] = data[0];
	
	for (var i = 1; i < (requiredSize - 1); i ++) {
		var newIndex = Math.round((i / (requiredSize - 2)) * (dataCount - 2));
		
		if (oldIndex === newIndex) {
			indicesToUpdate.push(i);
		} else {
			newData[i] = data[newIndex];
		}
		
		oldIndex = newIndex;
	}
	
	newData[requiredSize - 1] = data[dataCount - 1];
	
	var n = 0;
	
	for (var j = 0; j < requiredSize; j ++) {
		if (indicesToUpdate[n] === j) {
			newData[j] = getAveragePoint(newData[j - 1], newData[j + 1]);
			n ++;
		}
		
		if (n == indicesToUpdate.length) {
			break;
		}
	}

	return newData;
}

function getAveragePoint (point1, point2) {
	var averagePoint = {};
	
	if (point1 === undefined || point2 === undefined) {
		console.log('UNDEFINED POINT');
	}
	
	averagePoint['jd'] = (point1['jd'] + point2['jd']) / 2.0;
	averagePoint['mag'] = (point1['mag'] + point2['mag']) / 2.0;
	averagePoint['magerr'] = (point1['magerr'] + point2['magerr']) / 2.0;
	averagePoint['airmass'] = (point1['airmass'] + point2['airmass']) / 2.0;
	
	return averagePoint;
}

function plainifyData (data) {
	var plainData = {};
	
	for (var i = 0; i < data.length; i ++) {
		plainData['jd' + i] = data[i]['jd'];
		plainData['mag' + i] = data[i]['mag'];
		
		if (data[i]['magerr']) {
			plainData['magerr' + i] = data[i]['magerr'];
		} else {
			plainData['magerr' + i] = "";
		}
		
		if (data[i]['airmass']) {
			plainData['airmass' + i] = data[i]['airmass'];
		} else {
			plainData['airmass' + i] = "";
		}
	}
	
	return plainData;
}