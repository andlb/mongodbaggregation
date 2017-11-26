var pipeline = {$project:{"size":$split(title)}}

db.movies.aggregage([{$project: {$split: ["title"," "] }    }])

db.movies.aggregate([{$project:{
					  title:1, 
					  splt:{$size:{$split:["$title"," "]} } 
					  }},
					  $match:{splt:}
					  ]).itcount()

db.movies.aggregate([{$project:{
					  title:1, 
					  splt:{$size:{$split:["$title"," "]} } 
					  }},
					  {$match:{splt:1}}
					  ]).itcount()
					{$project:{cast:1,num_favs: { $setUnion: [ "$cast" ,"$favorites"]}}}
					
db.movies.aggregate([
					{"$match":{"cast":{$ne:null},"tomatoes.viewer.rating":{$gte:3}}},			
					{"$addFields":{"favorites":["Sandra Bullock","Tom Hanks","Julia Roberts","Kevin Spacey","George Clooney"]}},
					{$project:{"title":1,"countries":1,"num_favs": {$size: { $setIntersection: [ "$cast", "$favorites" ] }}}},
					{$sort:{num_favs:-1,"tomatoes.viewer.rating":-1}}
					])
					

db.movies.aggregate([
					{"$match":{"awards":{$ne:null},"awards" : {$regex : /Won.*Oscar/}}},					
					{"$group":{"_id":null,
						"highest_rating": { "$max": "$imdb.rating" },
						"lowest_rating": { "$min": "$imdb.rating" },
						"average_rating": { "$avg": "$imdb.rating" },
						"deviation": { "$stdDevSamp": "$imdb.rating" }}}
					])
db.movies.aggregate([
					{"$match":{"languages":{$in:["English"]}}},	
					{"$unwind":"$cast"},
					{"$group":{"_id":"$cast","numFilms":{$sum:1},"average": { "$avg": "$imdb.rating"}}},
					{"$sort": { "numFilms": -1 }},
					{"$limit": 1}
					])
					

db.air_alliances.aggregate([
						{"$lookup":{
							"from":"air_routes",
							"localField":"airlines",
							"foreignField":"airline.name",
							"as":"alliance"}
						},
						{"$unwind":"$alliance"},
						{"$match":{"alliance.airplane":{"$in":["747","380"]}}},
						{"$group":{"_id":"$name","qtd":{$sum:1}}}
						]).pretty()
						

						

db.air_alliances.aggregate([
						{"$match":{"name":"OneWorld"}},
						{"$graphLookup":{
							"startWith":"$airlines",
							"from":"air_airlines",
							"connectFromField":"name",
							"connectToField":"name",
							"as":"airlines",
							maxDepth:0,
							restrictSearchWithMatch:{
								country: { $in: ["Germany","Spain","Canada"]}
							}
						}},
						{"$graphLookup":{
							"startWith":"$airlines.base",
							"from":"air_routes",
							"connectFromField":"dst_airport",
							"connectToField":"src_airport",
							"as":"connections",
							maxDepth:1,							
						}},
						{"$project":{
							validAirLines:"$airlines.name",
							"connections.dst_airport":1,
							"connections.airline.name":1
						}},
						{$unwind:"$connections"},
						{$project:{
							isValid: { $in: ["$connections.airline.name","$validAirLines"]},
							"connections.dst_airport":1
							}
						},
						{$match:{isValid:true}},
						{$group:{"_id": "$connections.dst_airport", "qtd":{$sum:1}}}
						]).pretty()

db.air_airlines.aggregate([
						{"$match":{country: { $in: ["Germany","Spain","Canada"]}}},
						{"$lookup":{
							"from":"air_alliances",
							"foreignField":"airlines",
							"localField":"name",
							"as":"alliance"
						}},
						{"$match":{"alliance.name":"OneWorld"}},
						{"$graphLookup":{
							"startWith":"$base",
							"from":"air_routes",
							"connectFromField":"dst_airport",
							"connectToField":"src_airport",
							"as":"connections",
							maxDepth:1,							
						}},
						{$project:{
							"connections.dst_airport":1
							}
						},
						{$unwind:"$connections"},
						{$group:{"_id": null, "qtd":{$sum:1}}}
						])
					{$project:{title:1}},
						


db.movies.aggregate([
					{"$match":{"imdb.rating":{$gte:0}, "metacritic":{$ne:null}}},
					{$project:{title:1,'imdb.rating':1}},
					{$sort:{'imdb.rating':-1}},
					{$limit:10},
					{$group:{_id:null,"titulos":{$push:"$title"}}},
					
					])
					
//tem que achar o top 10 de um e deopis achar o top 10 do outro.
db.movies.aggregate([{
						$facet:{
						"categorizedByRating": [
								{$project:{title:1}},
								{ $sort: {"imdb.rating":-1} },
								
							  ],
						"categorizedBymetacritic": [
								{$project:{title:1}},
								{ $sort: {"metacritic":-1} },
								
							  ]
						}
					}])


db.movies.aggregate([
					{"$match":{"imdb.rating":{$gte:0}, "metacritic":{$ne:null}}},
					{"$group":{"_id":{"titleimdb":"$title"},"max_imdb": { "$max": "$imdb.rating"},"max_metacritic": { "$max": "$metacritic"}}},
					{"$sort":{"max_imdb":-1}},					
					{"$group":{"_id":"$_id.titleimdb","max_metacritic": { "$max": "$max_metacritic"}}},					
					])
					
					{"$sort":{"max_metacritic":-1}}

db.movies.aggregate([
					{"$match":{"languages":{$in:["English"]}}},	
					{"$unwind":"$cast"},
					{"$group":{"_id":"$cast","numFilms":{$sum:1},"average": { "$avg": "$imdb.rating"}}},
					{"$sort": { "numFilms": -1 }},
					{"$limit": 1}
					])		

					